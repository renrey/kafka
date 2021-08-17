/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.{Closeable, File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{ByteBufferUnmapper, OperatingSystem, Utils}

/**
 * The abstract index class which holds entry format agnostic methods.
 *
 * @param _file The index file
 * @param baseOffset the base offset of the segment that this index is corresponding to.
 * @param maxIndexSize The maximum index size in bytes.
 */
abstract class AbstractIndex(@volatile private var _file: File, val baseOffset: Long, val maxIndexSize: Int = -1,
                             val writable: Boolean) extends Closeable {
  import AbstractIndex._

  // Length of the index file
  @volatile
  private var _length: Long = _
  protected def entrySize: Int  // 一个entry的大小

  /*
   通过mmap，映射index文件到内存，所有的读写操作都是在OS page cache上。可以避免磁盘io的堵塞
   Kafka mmaps index files into memory, and all the read / write operations of the index is through OS page cache. This
   avoids blocked disk I/O in most cases.

   一般操作系统都是lru或者它的变种来管理page cache（页缓存）。而kafka都是追加写到index文件的末尾，并且一些查找操作都是接近文件末尾的。所以
   这个类似lru的处理，会运行得挺不错（就是一般不会去找前面的要被淘汰的）。
   To the extent of our knowledge, all the modern operating systems use LRU policy or its variants to manage page
   cache. Kafka always appends to the end of the index file, and almost all the index lookups (typically from in-sync
   followers or consumers) are very close to the end of the index. So, the LRU cache replacement policy should work very
   well with Kafka's index access pattern.

   然而传统的二分查找算法对缓存来说不太友好。会导致一些没必要的page fault错误，比如当一些entry没有在page cache时，线程会堵塞等待从硬盘读取这些entry
   However, when looking up index, the standard binary search algorithm is not cache friendly, and can cause unnecessary
   page faults (the thread is blocked to wait for reading some index entries from hard disk, as those entries are not
   cached in the page cache).

   For example, in an index with 13 pages, to lookup an entry in the last page (page #12), the standard binary search
   algorithm will read index entries in page #0, 6, 9, 11, and 12.
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12 |
   steps:       |1| | | | | |3| | |4|  |5 |2/6|
   In each page, there are hundreds log entries, corresponding to hundreds to thousands of kafka messages. When the
   index gradually growing from the 1st entry in page #12 to the last entry in page #12, all the write (append)
   operations are in page #12, and all the in-sync follower / consumer lookups read page #0,6,9,11,12. As these pages
   are always used in each in-sync lookup, we can assume these pages are fairly recently used, and are very likely to be
   in the page cache. When the index grows to page #13, the pages needed in a in-sync lookup change to #0, 7, 10, 12,
   and 13:
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13 |
   steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|

   新增page cache后，可能新的二分查找新增的位置（不是末尾），page cache并没有加载到内存中，需要重新把他们从磁盘加载进来内存，时间至少需要一秒，造成至少一次延迟，大概一秒左右。
   Page #7 and page #10 have not been used for a very long time. They are much less likely to be in the page cache, than
   the other pages. The 1st lookup, after the 1st index entry in page #13 is appended, is likely to have to read page #7
   and page #10 from disk (page fault), which can take up to more than a second. In our test, this can cause the
   at-least-once produce latency to jump to about 1 second from a few ms.

   因此，这里有对缓存更友好的二分查找：
   如果目标在end-N后，就是end-N，end范围查找，不然就是start，end-N，这就能在固定的page cache中查找
   Here, we use a more cache-friendly lookup algorithm:
   if (target > indexEntry[end - N]) // if the target is in the last N entries of the index
      binarySearch(end - N, end)
   else
      binarySearch(begin, end - N)

   如果可以的话，就只在最后N个entry进行查找。
   最好所有in-sync同步查找应该在前面的分支进行（begin, end - N）（因为查找都是一样的）。所以后面的N个就是我们叫做热区（warm section）。而我们一般频繁
   在热区的这些缓存页中查找，所以他们会比较长时间在内存中。
   If possible, we only look up in the last N entries of the index. By choosing a proper constant N, all the in-sync
   lookups should go to the 1st branch. We call the last N entries the "warm" section. As we frequently look up in this
   relatively small section, the pages containing this section are more likely to be in the page cache.

   N - _warmEntries：8192
   原因：
   1。这个值足够小，可以保证所有热区的page都被涉及到热区相关的搜索。
   当做热区查找的时候，有3个entry会一直被涉及到：end[未]，end-N[头]，(end*2 -N)/2[中间，end-2/N]，如果page的大小 >= 4096,所有热区页都被读取，
   因为在2018年的时候，所有处理器最小（读取）页单位就是4096（4k），所以4096*2（头页到尾的距离）。
   2。这个值足够大（数据足够多）可以保证大多数的in-sync查找在热区进行。
   对应kakfa的默认配置，8k的index对应是4M的（offsetindex） 或者 2.7M的time index
   We set N (_warmEntries) to 8192, because
   1. This number is small enough to guarantee all the pages of the "warm" section is touched in every warm-section
      lookup. So that, the entire warm section is really "warm".
      When doing warm-section lookup, following 3 entries are always touched: indexEntry(end), indexEntry(end-N),
      and indexEntry((end*2 -N)/2). If page size >= 4096, all the warm-section pages (3 or fewer) are touched, when we
      touch those 3 entries. As of 2018, 4096 is the smallest page size for all the processors (x86-32, x86-64, MIPS,
      SPARC, Power, ARM etc.).
   2. This number is large enough to guarantee most of the in-sync lookups are in the warm-section. With default Kafka
      settings, 8KB index corresponds to about 4MB (offset index) or 2.7MB (time index) log messages.

   We can't set make N (_warmEntries) to be larger than 8192, as there is no simple way to guarantee all the "warm"
   section pages are really warm (touched in every lookup) on a typical 4KB-page host.

   未来可能使用后台定时接触热区（现在没有）
   In there future, we may use a backend thread to periodically touch the entire warm section. So that, we can
   1) support larger warm section 支持更大的热区
   2) make sure the warm section of low QPS topic-partitions are really warm. 确保低QPS的分区的热区真的热
 */
  protected def _warmEntries: Int = 8192 / entrySize // 热区的entry数量 ：8192B / 每个entry的长度

  protected val lock = new ReentrantLock

  /**
   * 基于os cache实现的文件映射到内存的技术
   * 就是把index文件映射到内存
   * 这个东西的读写都是基于os cache执行的
   */
  @volatile
  protected var mmap: MappedByteBuffer = {
    // 创建文件.index
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* pre-allocate the file if necessary */
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file */
      _length = raf.length()
      /**
       * 磁盘index文件与内存映射，返回一个MappedByteBuffer
       */
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), AbstractIndex)
    }
  }

  /**
   * The maximum number of entries this index can hold
   */
  @volatile
  private[this] var _maxEntries: Int = mmap.limit() / entrySize

  /** The number of entries in this index */
  @volatile
  protected var _entries: Int = mmap.position() / entrySize // 当前entry数量

  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries

  def file: File = _file

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   *
   * @param newSize new size of the index file
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        debug(s"Index ${file.getAbsolutePath} was not resized because it already has size $roundedNewSize")
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
          if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
            safeForceUnmap()
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          debug(s"Resized ${file.getAbsolutePath} to $roundedNewSize, position is ${mmap.position()} " +
            s"and limit is ${mmap.limit()}")
          true
        } finally {
          CoreUtils.swallow(raf.close(), AbstractIndex)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File): Unit = {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath, false)
    finally _file = f
  }

  /**
   * Flush the data in the index to disk
   */
  def flush(): Unit = {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    closeHandler()
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize(): Unit = {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes: Int = entrySize * _entries

  /** Close the index */
  def close(): Unit = {
    trimToValidSize()
    closeHandler()
  }

  def closeHandler(): Unit = {
    // On JVM, a memory mapping is typically unmapped by garbage collector.
    // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
    // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
    // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
   * @throws IndexOffsetOverflowException
   */
  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  /**
   * Check if a particular offset is valid to be appended to this index.
   * @param offset The offset to check
   * @return true if this offset is valid to be appended to this index; false otherwise
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  protected def safeForceUnmap(): Unit = {
    if (mmap != null) {
      try forceUnmap()
      catch {
        case t: Throwable => error(s"Error unmapping index $file", t)
      }
    }
  }

  /**
   * Forcefully free the buffer's mmap.
   */
  protected[log] def forceUnmap(): Unit = {
    try ByteBufferUnmapper.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows or z/OS. We do this
   * because Windows or z/OS won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n the slot
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *
   * @param idx The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * Lookup lower and upper bounds for the given target.
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): (Int, Int) = {
    // check if the index is empty
    if(_entries == 0)
      return (-1, -1)

    /**
     * 二分查找entry（目标offset在index中对应entry）
     * @param begin
     * @param end
     * @return 1：entry
     *         2：
     */
    def binarySearch(begin: Int, end: Int) : (Int, Int) = {
      // binary search for the entry
      var lo = begin
      var hi = end
      while(lo < hi) {
        val mid = (lo + hi + 1) >>> 1
        val found = parseEntry(idx, mid)
        val compareResult = compareIndexEntry(found, target, searchEntity)
        // target在found前，更新右边界为中间
        if(compareResult > 0)
          hi = mid - 1
        // target在found后，更新左边界为中间
        else if(compareResult < 0)
          lo = mid
        else
          return (mid, mid)
      }
      // 1：左边界 2：左边界+1
      // 如果是左边界是末尾，那么2为-1
      (lo, if (lo == _entries - 1) -1 else lo + 1)
    }

    /**
     * index中获取第一个热区的entry下标 ：当前entry数量-1-热区entry数量 （end-N，end就是entry数-1）
     * 可以看看_warmEntries的注释！！！
     * _warmEntries：热区entry的数量
     */
    val firstHotEntry = Math.max(0, _entries - 1 - _warmEntries)
    // check if the target offset is in the warm section of the index
    /**
     * 目标offset在热区entry范围内，在热区范围内进行二分查找（end-N，end）
     */
    if(compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
      return binarySearch(firstHotEntry, _entries - 1)
    }

    // check if the target offset is smaller than the least offset
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    /**
     * 否则在前面的非热区范围进行二分查找（0，end-N）
     */
    binarySearch(0, firstHotEntry)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchType): Int = {
    searchEntity match {
      case IndexSearchType.KEY => java.lang.Long.compare(indexEntry.indexKey, target)
      case IndexSearchType.VALUE => java.lang.Long.compare(indexEntry.indexValue, target)
    }
  }

  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundDownToExactMultiple(67, 8) == 64
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

}

object AbstractIndex extends Logging {
  override val loggerName: String = classOf[AbstractIndex].getName
}

sealed trait IndexSearchType
object IndexSearchType {
  case object KEY extends IndexSearchType
  case object VALUE extends IndexSearchType
}
