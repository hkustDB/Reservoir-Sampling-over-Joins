import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.file.{FileSystems, Files}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer
import scala.io.Source

object SNBDataPreprocess {
    val insertInternalReportMarkers = true
    val inputDir = "/path/to/ldbc_snb_datagen_spark/out/graphs/csv/raw/composite-merged-fk"
    val staticDir: String = inputDir + "/static"
    val dynamicDir: String = inputDir + "/dynamic"

    val outputFile = "/path/to/data/snb.dat"

    val relationToSidMap: Map[String, Int] = Map(
        // static
        "tag_class" -> 0, "tag1" -> 1, "tag2" -> 2, "country" -> 3, "city" -> 4,
        // dynamic
        "message" -> 5, "message_has_tag1" -> 6, "message_has_tag2" -> 7, "person1" -> 8, "person2" -> 9, "person_knows_person" -> 10
    )

    // e.g., 2012-11-26T02:21:29.262+08:00
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ");

    // for sjoin
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)))

    def write(relation: String, content: String) = {
        writer.write(appendSid(relation, content))
        writer.newLine()
    }

    def appendSid(relation: String, content: String) = {
        s"${relationToSidMap(relation)}|1,${content}"
    }

    def wrapString(s: String): String = {
        "\"" + s + "\""
    }

    def datetimeToTimestamp(datetime: String): Long = {
        Instant.from(formatter.parse(datetime)).toEpochMilli
    }

    def listPartFiles(path: String): List[String] = {
        val dir = FileSystems.getDefault.getPath(path)
        Files.list(dir).iterator().asScala.toList.map(p => p.getFileName.toString).filter(n => n.startsWith("part-"))
    }

    def main(args: Array[String]): Unit = {
        // tag_class: id|name|url|SubclassOfTagClassId => id,"name","url",SubclassOfTagClassId
        for (tagClassPartFile <- listPartFiles(staticDir + "/TagClass")) {
            val tagClassSource = Source.fromFile(staticDir + "/TagClass/" + tagClassPartFile)
            val tagClassLines = tagClassSource.getLines().toList.tail
            tagClassSource.close()
            for (tagClassLine <- tagClassLines) {
                val arr = tagClassLine.split("\\|", Int.MaxValue)
                write("tag_class", s"${arr(0)},${wrapString(arr(1))},${wrapString(arr(2))},${arr(3)}")
            }
        }

        // tag: id|name|url|TypeTagClassId => id,"name","url",TypeTagClassId
        val tagBuffer = ListBuffer.empty[String]
        for (tagPartFile <- listPartFiles(staticDir + "/Tag")) {
            val tagSource = Source.fromFile(staticDir + "/Tag/" + tagPartFile)
            val tagLines = tagSource.getLines().toList.tail
            tagSource.close()
            for (tagLine <- tagLines) {
                val arr = tagLine.split("\\|", Int.MaxValue)
                tagBuffer.append(s"${arr(0)},${wrapString(arr(1))},${wrapString(arr(2))},${arr(3)}")
            }
        }
        for (tag <- tagBuffer) {
            write("tag1", tag)
        }
        for (tag <- tagBuffer) {
            write("tag2", tag)
        }
        tagBuffer.clear()

        // place: id|name|url|type|PartOfPlaceId
        // city: id|name|url|City|PartOfPlaceId => id,"name","url",PartOfPlaceId
        // country id|name|url|Country|PartOfPlaceId => id,"name","url",PartOfPlaceId
        val cityBuffer = ListBuffer.empty[String]
        val countryBuffer = ListBuffer.empty[String]
        for (placePartFile <- listPartFiles(staticDir + "/Place")) {
            val placeSource = Source.fromFile(staticDir + "/Place/" + placePartFile)
            val placeLines = placeSource.getLines().toList.tail
            placeSource.close()
            for (placeLine <- placeLines) {
                val arr = placeLine.split("\\|", Int.MaxValue)
                arr(3) match {
                    case "Country" =>
                        val content = s"${arr(0)},${wrapString(arr(1))},${wrapString(arr(2))},${arr(4)}"
                        countryBuffer.append(content)
                    case "City" =>
                        val content = s"${arr(0)},${wrapString(arr(1))},${wrapString(arr(2))},${arr(4)}"
                        cityBuffer.append(content)
                    case _ =>
                }
            }
        }
        for (country <- countryBuffer) {
            write("country", country)
        }
        countryBuffer.clear()
        for (city <- cityBuffer) {
            write("city", city)
        }
        cityBuffer.clear()

        // dynamic begins
        // use a ListBuffer to store (creationDate, content). Sort the buffer before finally writing to file
        val dynamicBuffer = ListBuffer.empty[(Long, String)]

        // message: Either[Post,Comment]
        // Post: creationDate|deletionDate|explicitlyDeleted|id|imageFile|locationIP|browserUsed|language|content|length|CreatorPersonId|ContainerForumId|LocationCountryId
        //      => id,"locationIP","browserUsed","content",length,CreatorPersonId
        for (postPartFile <- listPartFiles(dynamicDir + "/Post")) {
            val postSource = Source.fromFile(dynamicDir + "/Post/" + postPartFile)
            val postLines = postSource.getLines().toList.tail
            postSource.close()
            for (postLine <- postLines) {
                val arr = postLine.split("\\|")
                val creationDate = datetimeToTimestamp(arr(0))
                val content = s"${arr(3)},${wrapString(arr(5))},${wrapString(arr(6))},${wrapString(arr(8))},${arr(9)},${arr(10)}"
                dynamicBuffer.append((creationDate, appendSid("message", content)))
            }
        }

        // Comment: creationDate|deletionDate|explicitlyDeleted|id|locationIP|browserUsed|content|length|CreatorPersonId|LocationCountryId|ParentPostId|ParentCommentId
        //      => id,"locationIP","browserUsed","content",length,CreatorPersonId
        for (commentPartFile <- listPartFiles(dynamicDir + "/Comment")) {
            val commentSource = Source.fromFile(dynamicDir + "/Comment/" + commentPartFile)
            val commentLines = commentSource.getLines().toList.tail
            commentSource.close()
            for (commentLine <- commentLines) {
                val arr = commentLine.split("\\|")
                val creationDate = datetimeToTimestamp(arr(0))
                val content = s"${arr(3)},${wrapString(arr(4))},${wrapString(arr(5))},${wrapString(arr(6))},${arr(7)},${arr(8)}"
                dynamicBuffer.append((creationDate, appendSid("message", content)))
            }
        }

        // PostHasTag: creationDate|deletionDate|PostId|TagId => MessageId,TagId
        for (postHasTagPartFile <- listPartFiles(dynamicDir + "/Post_hasTag_Tag")) {
            val postHasTagSource = Source.fromFile(dynamicDir + "/Post_hasTag_Tag/" + postHasTagPartFile)
            val postHasTagLines = postHasTagSource.getLines().toList.tail
            postHasTagSource.close()
            for (postHasTagLine <- postHasTagLines) {
                val arr = postHasTagLine.split("\\|")
                val creationDate = datetimeToTimestamp(arr(0))
                val content = s"${arr(2)},${arr(3)}"
                dynamicBuffer.append((creationDate, appendSid("message_has_tag1", content)))
                dynamicBuffer.append((creationDate, appendSid("message_has_tag2", content)))
            }
        }

        // CommentHasTag: creationDate|deletionDate|CommentId|TagId => MessageId,TagId
        for (commentHasTagPartFile <- listPartFiles(dynamicDir + "/Comment_hasTag_Tag")) {
            val commentHasTagSource = Source.fromFile(dynamicDir + "/Comment_hasTag_Tag/" + commentHasTagPartFile)
            val commentHasTagLines = commentHasTagSource.getLines().toList.tail
            commentHasTagSource.close()
            for (commentHasTagLine <- commentHasTagLines) {
                val arr = commentHasTagLine.split("\\|")
                val creationDate = datetimeToTimestamp(arr(0))
                val content = s"${arr(2)},${arr(3)}"
                dynamicBuffer.append((creationDate, appendSid("message_has_tag1", content)))
                dynamicBuffer.append((creationDate, appendSid("message_has_tag2", content)))
            }
        }

        // Person: creationDate|deletionDate|explicitlyDeleted|id|firstName|lastName|gender|birthday|locationIP|browserUsed|LocationCityId
        //      => id,"firstName","lastName","gender","birthday","locationIP","browserUsed",LocationCityId
        for (personPartFile <- listPartFiles(dynamicDir + "/Person")) {
            val personSource = Source.fromFile(dynamicDir + "/Person/" + personPartFile)
            val personLines = personSource.getLines().toList.tail
            personSource.close()
            for (personLine <- personLines) {
                val arr = personLine.split("\\|")
                val creationDate = datetimeToTimestamp(arr(0))
                val content = s"${arr(3)},${wrapString(arr(4))},${wrapString(arr(5))},${wrapString(arr(6))}," +
                    s"${wrapString(arr(7))},${wrapString(arr(8))},${wrapString(arr(9))},${arr(10)}"
                dynamicBuffer.append((creationDate, appendSid("person1", content)))
                dynamicBuffer.append((creationDate, appendSid("person2", content)))
            }
        }

        // PersonKnowsPerson: creationDate|deletionDate|explicitlyDeleted|Person1Id|Person2Id
        //      => Person1Id,Person2Id
        for (personKnowsPersonPartFile <- listPartFiles(dynamicDir + "/Person_knows_Person")) {
            val personKnowsPersonSource = Source.fromFile(dynamicDir + "/Person_knows_Person/" + personKnowsPersonPartFile)
            val personKnowsPersonLines = personKnowsPersonSource.getLines().toList.tail
            personKnowsPersonSource.close()
            for (personKnowsPersonLine <- personKnowsPersonLines) {
                val arr = personKnowsPersonLine.split("\\|")
                val creationDate = datetimeToTimestamp(arr(0))
                val content1 = s"${arr(3)},${arr(4)}"
                dynamicBuffer.append((creationDate, appendSid("person_knows_person", content1)))
                val content2 = s"${arr(4)},${arr(3)}"
                dynamicBuffer.append((creationDate, appendSid("person_knows_person", content2)))
            }
        }

        val sortedDynamic = dynamicBuffer.toList.sortBy(t => t._1)
        val dynamicSize = sortedDynamic.size
        val factor = 0.10
        val step = Math.ceil(dynamicSize * factor).toInt + 1
        var nextTagSize = step
        var cnt = 0
        for (tuple <- sortedDynamic) {
            writer.write(tuple._2)
            writer.newLine()

            cnt += 1
            if (insertInternalReportMarkers) {
                if (cnt > nextTagSize) {
                    nextTagSize += step
                    writer.write(s"-")
                    writer.newLine()
                }
            }
        }
        writer.write(s"-")
        writer.newLine()
        writer.flush()
        writer.close()
    }

}
