package com.spark.sqs

import java.io.FileInputStream
import java.util.Properties

import awscala.sqs.{Queue, SQS}
import awscala.{File, Region}
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.model.CreateQueueRequest
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.annotation.tailrec

class SqsReceiver(name: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  private val credentials: SQSCredentials = new SQSCredentials
  private var region: Regions = Regions.US_EAST_2
  private var timeout: Int = 1000
 // private val resultMessage = ""

  def credentials(accessKeyId: String, secretAccessKey: String): SqsReceiver = {
    credentials.key(accessKeyId).secret(secretAccessKey)
    this
  }

  def credentials(filename: String): SqsReceiver = {
    val p = new PropertiesCredentials(new File(filename))
    credentials.key(p.getAWSAccessKeyId).secret(p.getAWSSecretKey)
    this
  }

  def at(region: Regions): SqsReceiver = {
    this.region = region
    this
  }

  def withTimeout(secs: Int) = {
    this.timeout = secs * 1000
    this
  }

  def onStart() {
    new Thread("SQS Receiver") {
      override def run() {
        try {

          implicit val sqs: SQS = (
            if (credentials.notValid)
                SQS.apply(credentials.key, credentials.secret)(Region.apply(region))
            else SQS.apply(credentials.key, credentials.secret)(Region.apply(region)))
                .at(Region.apply(region))
          //implicit val sqs : SQS = SQS.at(Region.apply(region))

          //println("Creating a new SQS queue called MyQueue.\n")
          val createQueueRequest = new CreateQueueRequest(name)
          val myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl
          println("\nReceiving messages from Queue: "+myQueueUrl)

          // List all queues.
     /*     println("\n********************Listing all queues in the account**********************\n")
          for (queueUrl <- sqs.listQueues.getQueueUrls) {
            println("  QueueUrl: " + queueUrl)
          }*/
          println()

          val queue: Queue = sqs.queue(name) match {
            case Some(q) => q
            case None    => throw new IllegalArgumentException(s"No queue with the name $name found")
          }

          @tailrec
          def poll(): Unit = {
            if (!isStopped()) {
              queue.messages.foreach(msg => {
                //println("step1")
                //println("msg: "+msg)
               //val resultMessage = msg.body
                //println("msg body: "+msg.getBody)
                //println("msg attrs: "+msg.attributes)
                //println("msg attrs: "+msg.messageAttributes)
                store(msg.body)
                //println("step2")
                //queue.remove(msg)
              })
              Thread.sleep(timeout)
             // println("step3")
              poll()

            }
          }
          //println("step4")
          poll()

        } catch {
          case e: IllegalArgumentException => restart(e.getMessage, e, 5000)
          case t: Throwable                => restart("Connection error", t)
        }
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling poll()
    // is designed to stop by itself isStopped() returns false
  }

  private class SQSCredentials extends Serializable {
    val prop = new Properties()
    prop.load(new FileInputStream("cred.properties"))


    private var _key = prop.getProperty("access_key")
    private var _secret = prop.getProperty("secret_key")
    def key = _key
    def secret = _secret
    def key(v: String) = {
      _key = v
      this
    }
    def secret(v: String) = {
      _secret = v
      this
    }
    def notValid = _key.isEmpty || _secret.isEmpty
  }

}