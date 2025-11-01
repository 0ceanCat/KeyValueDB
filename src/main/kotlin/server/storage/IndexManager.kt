package server.storage

import common.Utils
import org.slf4j.LoggerFactory
import server.writerReader.VerFReader
import server.writerReader.VerfWriter
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

object IndexManager {
    private val log = LoggerFactory.getLogger(IndexManager::class.java)
    private const val PATH = "index"
    private val verfWriter = VerfWriter()
    private val segmentsByLevel = LinkedBlockingQueue<SegmentsRef>() // store segments by their level and version
    private val version = AtomicInteger(0)

    init {
        scan()
    }

    private fun scan() {
        val (oldestVersion, segmentsIds) = VerFReader().readSegmentsIds()
        version.set(oldestVersion)
        val map = mutableMapOf<Int, MutableList<Segment>>()
        for (f in Utils.readFilesFrom(PATH) { it.startsWith(Segment.FILE_PREFIX) }) { // find all files whose name starts by 'segment'
            val id = Segment.getIdFromName(f.name)
            if (id in segmentsIds) {
                val segment = loadSegment(f)
                map.computeIfAbsent(segment.level) {
                    ArrayList()
                }.add(segment)
            } else {
                f.delete()
            }
        }
        for (segments in map.values) {
            segments.sortBy { segment -> segment.id }
        }
        segmentsByLevel.put(SegmentsRef(oldestVersion, map))
    }

    fun loadSegment(file: File): Segment {
        return Segment(file)
    }

    // called when a new segment file was written to disk
    fun loadNewSegmentAndNotifyMerger(name: String) {
        loadSegment(File(name)) // load it
        Merger.tryMerge() // wake up the Merger thread
    }

    fun remove(toBeDeleted: List<Segment>) {
        val last = segmentsByLevel.last()
        last.toBeDeleted = toBeDeleted
        val newVersionMap = SegmentsRef.clone(version.incrementAndGet(), last)
        for (segment in toBeDeleted) {
            newVersionMap[segment.level]?.remove(segment)
        }
        segmentsByLevel.add(newVersionMap)
        writeNewVersionToDisk(newVersionMap)
        cleanUpOldVersions()
    }

    private fun writeNewVersionToDisk(ref: SegmentsRef) {
        verfWriter.write(ref.version, ref.segments.values.stream().flatMap { it.stream() }.toList())
    }

    fun <T> startSearchIn(func: (Map<Int, List<Segment>>) -> T): T {
        val segmentsRef = segmentsByLevel.last()
        segmentsRef.ref()
        val result = func(segmentsRef.segments)
        segmentsRef.unRef()
        cleanUpOldVersions()
        return result
    }

    private fun cleanUpOldVersions() {
        var oldest = segmentsByLevel.peek()
        while (oldest.refCount() == 0 && oldest.version != version.get()) {
            val segmentsOldVersion = segmentsByLevel.poll()
            for (segment in segmentsOldVersion.toBeDeleted) {
                try {
                    segment.close()
                    segment.remove()
                } catch (e: Exception) {
                    log.error("Close/Delete Segment ${segment.path} failed", e)
                }
            }
            oldest = segmentsByLevel.peek()
        }
    }
}

private data class SegmentsRef(val version: Int, val segments: Map<Int, MutableList<Segment>>): Map<Int, MutableList<Segment>> by segments {
    private val reference = AtomicInteger(0)
    var toBeDeleted: List<Segment> = listOf()

    companion object {
        fun clone(version: Int, other: SegmentsRef): SegmentsRef {
            return SegmentsRef(version, HashMap(other.segments))
        }
    }

    fun refCount(): Int {
        return reference.get()
    }

    fun ref() {
        reference.incrementAndGet()
    }

    fun unRef() {
        reference.decrementAndGet()
    }
}