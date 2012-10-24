import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.glacier.model.{ListVaultsRequest, DeleteVaultRequest, CreateVaultRequest, DeleteArchiveRequest}
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sqs.AmazonSQSClient
import java.io.File;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager
import java.util.{Properties, Date}
import collection.JavaConverters._

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
}

object Khumbu extends App {
  val khumbu = new Khumbu()
  val vaultName = "testVault"
  khumbu.listVaults()
  khumbu.createVault(vaultName)
  val id = khumbu.upload(vaultName, "test.file")
  println("archiveId from upload : " + id)
  khumbu.download(vaultName, id)
  khumbu.delete(vaultName, id)
//  khumbu.deleteVault("testVault") // will break because vault was just written to
  
}