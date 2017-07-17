package org.opentosca.NodeTypes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;

import javax.jws.Oneway;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.xml.bind.annotation.XmlElement;

import org.vngx.jsch.Session;
import org.vngx.jsch.exception.JSchException;

import ssh.SSHRemoteCommandExec;
import ssh.SSHRemoteFileTransfer;
import ssh.SSHSessionFactory;

@WebService(targetNamespace = "http://implementationartifacts.opentosca.org/")
public class org_opentosca_NodeTypes_Ubuntu extends AbstractIAService {

	public static final String MSG_FAILED = "FAILED";
	public static final String TESTMODE = "TESTMODE";

	@WebMethod
	@SOAPBinding
	@Oneway
	public void installPackage(
			@WebParam(name = "VMIP", targetNamespace = "http://implementationartifacts.opentosca.org/") String IP,
			@WebParam(name = "VMUserName", targetNamespace = "http://implementationartifacts.opentosca.org/") String User,
			@WebParam(name = "VMPrivateKey", targetNamespace = "http://implementationartifacts.opentosca.org/") String PrivateKey,
			@WebParam(name = "PackageNames", targetNamespace = "http://implementationartifacts.opentosca.org/") String PackageNames) {
		// This HashMap holds the return parameters of this operation.
		final HashMap<String, String> returnParameters = new HashMap<String, String>();

	Session session = SSHSessionFactory.createSSHSession(IP.trim(), User.trim(), PrivateKey.trim());
		
		// just to be sure the packages will be installed either with apt-get or
		// yum
		String installPackageScript = "(sudo apt-get update && sudo apt-get -y install " + PackageNames + ") || (sudo yum update && sudo yum -y install " + PackageNames + ")";
		String res = this.runScript(session, installPackageScript).trim();
		// Output Parameter 'success' (optional)
		// Do NOT delete the next line of code. Set "" as value if you want to
		// return nothing or an empty result!
		if (res.endsWith("Complete!") || res.endsWith("Nothing to do")) {
			returnParameters.put("InstallResult", "1");
			
		} else {
			returnParameters.put("InstallResult", "0");
		}

		sendResponse(returnParameters);
	}

	@WebMethod
	@SOAPBinding
	@Oneway
	public void transferFile(
			@WebParam(name = "VMIP", targetNamespace = "http://implementationartifacts.opentosca.org/") String IP,
			@WebParam(name = "VMUserName", targetNamespace = "http://implementationartifacts.opentosca.org/") String User,
			@WebParam(name = "VMPrivateKey", targetNamespace = "http://implementationartifacts.opentosca.org/") String  PrivateKey,
			@WebParam(name = "TargetAbsolutePath", targetNamespace = "http://implementationartifacts.opentosca.org/") String TargetAbsolutePath,
			@WebParam(name = "SourceURLorLocalPath", targetNamespace = "http://implementationartifacts.opentosca.org/") String SourceURLorLocalPath) {
		// This HashMap holds the return parameters of this operation.
		final HashMap<String, String> returnParameters = new HashMap<String, String>();

		// Transform sourceURLorLocalAbsolutePath to URL
		URL url = null;
		try {
			// Check if the string is a URL right away?
			url = new URL(SourceURLorLocalPath);
		} catch (Exception e) {
			// It's not a URL
			// Check if string is a local path
			File file = new File(SourceURLorLocalPath);
			if (file.exists()) {
				try {
					url = file.toURI().toURL();

				} catch (Exception e2) {
					// FAILED: Return async message
					// Also problem processing as file, giving up
					returnParameters.put("TransferResult", "TRANSFER_FAILED: File " + SourceURLorLocalPath
							+ " is no valid URL and does not exist on the local file system. (Exception: " + e + ")");
					this.sendResponse(returnParameters);
					return;
				}
			} else {
				// FAILED: Return async message
				returnParameters.put("TransferResult", "TRANSFER_FAILED: File " + SourceURLorLocalPath
						+ " is no valid URL and does not exist on the local file system.");
				this.sendResponse(returnParameters);
				return;
			}
		}

		// Resolve to user home if remote path starts with ~
		if (TargetAbsolutePath.startsWith("~")) {
			Session session = SSHSessionFactory.createSSHSession(IP, User, PrivateKey);
			String pwd = this.runScript(session, "pwd").trim();
			TargetAbsolutePath = TargetAbsolutePath.replaceFirst("~", pwd);
			System.out.println("Replaced ~ with user home ('" + pwd + "'): '" + TargetAbsolutePath + "'");
			session.disconnect();
		}

		// Opens stream and uploads file
		try {
			// If there is no output stream a HTTP GET is done by default
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestProperty("Accept", "application/octet-stream");
			InputStream inputStream = httpConnection.getInputStream();

			Session session = SSHSessionFactory.createSSHSession(IP, User, PrivateKey);
			SSHRemoteFileTransfer transfer = new SSHRemoteFileTransfer(session);
			transfer.putFile(inputStream, TargetAbsolutePath);
			transfer.close();

			inputStream.close();
			httpConnection.disconnect();
			session.disconnect();

			returnParameters.put("TransferResult", "sucessfull");

		} catch (Exception e) {
			e.printStackTrace();
			returnParameters.put("TransferResult", "TRANSFER_FAILED: " + e);
		}

		// Returning a parameter is required so that we can wait asynchronously
		// in the process.

		sendResponse(returnParameters);
	}

	@WebMethod
	@SOAPBinding
	@Oneway
	public void runScript(
			@WebParam(name = "VMIP", targetNamespace = "http://implementationartifacts.opentosca.org/") String IP,
			@WebParam(name = "VMUserName", targetNamespace = "http://implementationartifacts.opentosca.org/") String User,
			@WebParam(name = "VMPrivateKey", targetNamespace = "http://implementationartifacts.opentosca.org/") String PivateKey,
			@WebParam(name = "Script", targetNamespace = "http://implementationartifacts.opentosca.org/") String Script) {
		// This HashMap holds the return parameters of this operation.
		final HashMap<String, String> returnParameters = new HashMap<String, String>();

		Session session = SSHSessionFactory.createSSHSession(IP, User, PivateKey);
		String res = this.runScript(session, Script);

		returnParameters.put("ScriptResult", res);

		this.sendResponse(returnParameters);
	}

	@WebMethod
	@SOAPBinding
	@Oneway
	public void waitForAvailability(
			@WebParam(name = "VMIP", targetNamespace = "http://implementationartifacts.opentosca.org/") String IP,
			@WebParam(name = "VMUserName", targetNamespace = "http://implementationartifacts.opentosca.org/") String User,
			@WebParam(name = "VMPrivateKey", targetNamespace = "http://implementationartifacts.opentosca.org/") String PrivateKey) {
		// This HashMap holds the return parameters of this operation.
		final HashMap<String, String> returnParameters = new HashMap<String, String>();

		// Testmode
		if (IP.equals(TESTMODE)) {
			System.out.println("##### " + TESTMODE + " ##### ");
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// Output Parameter 'pwd' (optional)
			// Do NOT delete the next line of code. Set "" as value if you want
			// to return nothing or an empty result!
			returnParameters.put("WaitResult", TESTMODE);
			this.sendResponse(returnParameters);
			return;
		}

		// Try to connect to SSH port (for approx. 16 min)
		int maxTriesSocket = 100;
		for (int i = 1; i <= maxTriesSocket; i++) {
			if (this.isSSHServiceUp(IP)) {
				break;
			}

			System.out.println("Waiting for SSH service to come up... (" + i + " of max. " + maxTriesSocket + ")");

			// Wait
			try {
				Thread.sleep(5 * 1000); // wait for 5 sec
			} catch (InterruptedException e) {
				// we just go on in this case.
			}
		}
		System.out.println("SSH service is up, try to login.");

		// Try to establish SSH connection
		String pwd = "";
		int maxTriesSSHLogin = 25;
		for (int i = 1; i <= maxTriesSSHLogin; i++) {
			String pwdRespone = this.isSSHLoginPossible(IP, User, PrivateKey);
			if (!pwdRespone.equals(MSG_FAILED)) {
				pwd = pwdRespone;
				break;
			}

			System.out.println("Waiting for successfull SSH login... (" + i + " of max. " + maxTriesSSHLogin + ")");

			// Wait
			try {
				Thread.sleep(5 * 1000); // wait for 5 sec
			} catch (InterruptedException e) {
				// we just go on in this case.
			}
		}
		System.out.println("Ubuntu VM started and SSH is ready.");

		// Returning a parameter is required so that we can wait asynchronously
		// in the process.

		// Output Parameter 'pwd' (optional)
		// Do NOT delete the next line of code. Set "" as value if you want to
		// return nothing or an empty result!

		returnParameters.put("WaitResult", pwd);

		sendResponse(returnParameters);
	}

	/**
	 * Checks if the login via SSH is possible.
	 *
	 * @param hostname
	 * @param sshUser
	 * @param sshKey
	 * @return
	 */
	private String isSSHLoginPossible(String hostname, String sshUser, String sshKey) {
		// Try to establish SSH connection
		try {
			Session session = SSHSessionFactory.createSSHSession(hostname, sshUser, sshKey);
			SSHRemoteCommandExec cmd = new SSHRemoteCommandExec(session);
			/**
			 * Because of this problem we have to wait some time after we can
			 * declare the VM to be started up, because there might be running
			 * some background scripts, e.g. at Amazon to configure the APT get
			 * repositories
			 * http://serverfault.com/questions/440569/apt-get-update
			 * -directly-after-boot-results-in-many-ign-and-hit-resulting-in-no
			 */
			String res = cmd.execute("pwd; sleep 15;");
			cmd.close();
			return res;
		} catch (Exception e) {
			return MSG_FAILED;
		}
	}

	/**
	 * Checks if the SSH port allows a socket connection which indicated that
	 * the SSH service has been started.
	 *
	 * @return true if a socket connection could be created, false otherwise.
	 */
	private boolean isSSHServiceUp(String hostname) {
		// Try to open socket
		try {
			Socket s = new Socket(hostname, 22);
			s.close();
			return true;

		} catch (UnknownHostException e1) {
			// this exception is expected
			return false;

		} catch (IOException e1) {
			// this exception is expected
			return false;
		}
	}

	/**
	 * Run Script wrapper which returns the console output produced by the
	 * script.
	 *
	 * @param session
	 * @param script
	 * @return
	 */
	private String runScript(Session session, String script) {
		try {
			SSHRemoteCommandExec cmd = new SSHRemoteCommandExec(session);
			String res = cmd.execute(script);
			cmd.close();
			return res;
		} catch (JSchException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
