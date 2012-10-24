import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.glacier.model._
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sqs.AmazonSQSClient

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager
import java.io.{InputStream, File}
import java.util.Properties
import collection.JavaConverters._
import scala.io.Source

class Khumbu {
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("Khumbu.properties"))
  val region = properties.getProperty("region", "eu-west-1")
  val endpoint = "https://glacier." + region + ".amazonaws.com"
  val snsEndpoint = "https://sns." + region + ".amazonaws.com"
  val sqsEndpoint = "https://sqs." + region + ".amazonaws.com"
  /*
    Put Khumbu.properties to folder src/main/resources for this to work!
   */
  val credentials = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"))

  def client():AmazonGlacierClient = {
    val client = new AmazonGlacierClient(credentials)
    client.setEndpoint(endpoint)
    client
  }

  def manager() : ArchiveTransferManager = {
    val sns = new AmazonSNSClient(credentials);
    val sqs = new AmazonSQSClient(credentials);
    sns.setEndpoint(snsEndpoint)
    sqs.setEndpoint(sqsEndpoint)
    new ArchiveTransferManager(client(), sqs, sns)
  }

  def upload(vault: String, file:String) = {
    val result = manager().upload(vault, "test file to upload", new File(file))
    result.getArchiveId
  }

  def download(vault: String, id: String) {
    manager().download(vault, id, new File("/Users/miso/tmp/test.file"))
  }

  def delete(vault: String,  id: String) {
    client().deleteArchive(new DeleteArchiveRequest().withVaultName(vault).withArchiveId(id))
  }

  def createVault(vault: String) {
    client().createVault(new CreateVaultRequest(vault))
  }

  def deleteVault(vault: String) {
    client().deleteVault(new DeleteVaultRequest(vault))
  }

  def listVaults() {
    val vaultList = client().listVaults(new ListVaultsRequest()).getVaultList.asScala
    vaultList foreach {v => println(v)}
  }

  def startVaultInventoryJob(vault:String): String = {
    client().initiateJob(new InitiateJobRequest().withVaultName(vault).withJobParameters(new JobParameters().withType("inventory-retrieval"))).getJobId
  }

  def getInventory(vault: String, jobId: String) = {
    val is = client().getJobOutput(new GetJobOutputRequest().withVaultName(vault).withJobId(jobId))
    Source.fromInputStream(is.getBody).getLines().mkString("\n")
  }

  def waitForJob(vault: String, jobId: String) {
    val jobNotCompleted = !client().describeJob(new DescribeJobRequest().withVaultName(vault).withJobId(jobId)).isCompleted
    while (jobNotCompleted) {
      Thread.sleep(1000L * 60) // wait a minute
      print(".")
    }
  }
}

object Khumbu extends App {
  val khumbu = new Khumbu()
  val vaultName = "testVault"
  
  println("list vaults: ")
  khumbu.listVaults()
  
  println("creating vault (if it doesn't exist")
  khumbu.createVault(vaultName)

  println("uploading file")
  val id = khumbu.upload(vaultName, "test.file")
  println("archiveId from upload : " + id)

//  println("download file")
//  khumbu.download(vaultName, id)

  println("deleting file")
  khumbu.delete(vaultName, id)

//  println("deleting vault")
//  khumbu.deleteVault("testVault") // will break because vault was just written to

  val existingVault = "testing"

  println("start inventory job")
  val jobId = khumbu.startVaultInventoryJob(existingVault)
  println("inventory job id: " + jobId)

  println("poll for inventory")
  khumbu.waitForJob(existingVault, jobId)

  println("getting inventory")
  val inventory = khumbu.getInventory(existingVault, jobId)
  println(inventory)
}