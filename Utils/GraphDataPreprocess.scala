import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import scala.io.Source
import scala.util.Random

object GraphDataPreprocess {
    def main(args: Array[String]): Unit = {
        val inputFile = "/path/to/epinions.txt"
        val outputDir = "/path/to/data"

        val source = Source.fromFile(inputFile)
        val lines = source.getLines().toList
        val tuples = lines.map(l => {
            val arr = l.split(",")
            (arr(0), arr(1))
        })

        for (n <- 3 to 7) {
            val outputFile = outputDir + s"/epinions_${n}.txt"
            val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)))
            val buffer = tuples.flatMap(t => (0 until n).map(i => s"${i}|1,${t._1},${t._2}"))

            val shuffled = Random.shuffle(buffer)
            for (s <- shuffled) {
                writer.write(s)
                writer.newLine()
            }
            writer.write(s"-")
            writer.newLine()
            writer.flush()
            writer.close()

            // for Line-3 input size
            if (n == 3) {
                val outputFileProgress = outputDir + s"/epinions_${n}_progress.txt"
                val writerProgress = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileProgress)))
                val factor = 0.10
                val step = Math.ceil(shuffled.size * factor).toInt + 1
                var nextSize = step
                var cnt = 0
                for (s <- shuffled) {
                    writerProgress.write(s)
                    writerProgress.newLine()

                    cnt += 1
                    if (cnt > nextSize) {
                        nextSize += step
                        writerProgress.write(s"-")
                        writerProgress.newLine()
                    }
                }
                writerProgress.write(s"-")
                writerProgress.newLine()
                writerProgress.flush()
                writerProgress.close()
            }
        }

        source.close()
    }
}
