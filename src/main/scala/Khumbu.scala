import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sqs.AmazonSQSClient
import java.io.File;
import java.util.Date;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;

class Khumbu {
  lazy val vaultName = "testing"
  val region = "eu-west-1"
  lazy val endpoint = "glacier." + region + ".amazonaws.com"
  /*
    Put AwsCredentials.properties to folder src/main/resources for this to work!
   */
  lazy val credentials = new PropertiesCredentials(this.getClass.getResourceAsStream("AwsCredentials.properties"))
  val archiveId = "Q7gLvT9jCc6h0C04O0PnjdqAYrdtJ4sQ_IK3GbLZq4bFzevdaqNsS7VtBnFtfD7rt-JR-Rr7r1Lk3tO-XaVPbYGwCKQN-aRRSojD0AMT0Qiza8qq9CSV_qROI0WFTvzz3Q8YuoYHVw"
  def client():AmazonGlacierClient = {
    val client = new AmazonGlacierClient(credentials)
    client.setEndpoint(endpoint)
    client
  }
  def manager() : ArchiveTransferManager = {
    val sns = new AmazonSNSClient(credentials);
    val sqs = new AmazonSQSClient(credentials);
    sns.setEndpoint("sns." + region + ".amazonaws.com")
    sns.setEndpoint("sqs." + region + ".amazonaws.com")
    new ArchiveTransferManager(client(), sqs, sns)
  }

  def upload(file:String) {
    val result = manager().upload(vaultName, "archive at " + new Date(), new File(file))
    println("archive id for retrieval : " + result.getArchiveId)
  }

  def download() {
    manager().download(vaultName, archiveId, new File("/Users/miso/tmp/test.file"))
  }
}

object Khumbu extends App {
  //new Khumbu().upload("test.file")
  new Khumbu().download()
}