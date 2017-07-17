package org.opentosca.implementationartifacts;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.logging.Logger;

import org.opentosca.implementationartifacts.model.VMInstance;

import com.vmware.vim25.CustomizationFault;
import com.vmware.vim25.FileFault;
import com.vmware.vim25.GuestOperationsFault;
import com.vmware.vim25.GuestProgramSpec;
import com.vmware.vim25.InsufficientResourcesFault;
import com.vmware.vim25.InvalidDatastore;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.InvalidState;
import com.vmware.vim25.MigrationFault;
import com.vmware.vim25.NamePasswordAuthentication;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.TaskInProgress;
import com.vmware.vim25.VimFault;
import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRelocateSpec;
import com.vmware.vim25.VmConfigFault;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.GuestOperationsManager;
import com.vmware.vim25.mo.GuestProcessManager;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ResourcePool;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;

/**
 * @author Andreas Bader
 * @author Christian Endres
 *
 *         Code snippets from
 *         https://github.com/yavijava/yavijava-samples/blob/gradle/src/main/
 *         java/ samples/vm/CreateVM.java
 *         https://github.com/yavijava/yavijava-samples/blob/gradle/src/main/
 *         java/ samples/vm/VMpowerOps.java
 *         http://www.doublecloud.org/2012/02/run-program-in-guest-operating-
 *         system-on- vmware/
 *         https://github.com/yavijava/yavijava-samples/blob/gradle/src/main/
 *         java/ samples/vm/CloneVM.java
 */
public class vsphere_clone_template {

    private static final Logger log = Logger.getLogger(vsphere_clone_template.class.getName());

    // timeout for waiting for the guest tools on a created vm
    private final int waitTime = 3 * 60 * 1000;

    /**
     * Method for creating a VM by cloning a template.
     * 
     * @param regionEndpoint
     *            URL to the vSphere server, i.e.,
     *            https://iaasvc.informatik.uni-stuttgart.de/sdk
     * @param apiUser
     *            <userId>
     * @param apiPassword
     *            <password>
     * @param vsBasePath
     *            the basepath that vSphere requires
     * @param vsResourcePool
     *            the resource pool
     * @param templateID
     *            Name of the template to clone, i.e., m1.medium.template
     * @param vmUsername
     *            the username to create or use
     * @param vmUserPassword
     *            the password for the vmUsername
     * @param vmPublicKey
     *            The public key to inject
     * 
     * @throws RemoteException
     * @throws MalformedURLException
     * @throws InterruptedException
     * @throws CredentialsFormatException
     */
    public VMInstance create(String regionEndpoint, String apiUser, String apiPassword, String vsBasePath,
	String vsResourcePool, String templateID, String vmUsername, String vmUserPassword, String vmPublicKey)
	    throws RemoteException, MalformedURLException, InterruptedException, CredentialsFormatException {

	String vmName = "OT-ProvInstance-" + apiUser + "_" + System.currentTimeMillis();

	ServiceInstance vSphereConnection = connectToVSphere(regionEndpoint, apiUser, apiPassword);

	log.info("Searching root folder.");
	Folder rootFolder = vSphereConnection.getRootFolder();

	Datacenter dc = getDataCenter(vSphereConnection, rootFolder);
	ResourcePool rp = getResourcePool(vsResourcePool, vSphereConnection, rootFolder);
	VirtualMachine template = getVMTemplate(templateID, vSphereConnection, dc);
	log.info(vsBasePath);
	Folder vmFolder = getVMFolder(vSphereConnection, dc, vsBasePath);
	terminateIfExists(vsBasePath, vmName, vSphereConnection, vmFolder);

	VirtualMachine vm = cloneVM(vsBasePath, vmName, vSphereConnection, vmFolder, rp, template);

	waitForGuestTools(vSphereConnection, vm);

	String ip = vm.getSummary().getGuest().getIpAddress();
	log.info("VM has the name \"" + vmName + "\" at IP \"" + ip + "\" and the status of the guest tools is \""
	    + vm.getGuest().toolsRunningStatus + "\".");

	configureVM(vSphereConnection, vm, vmUsername, vmUserPassword, vmPublicKey);

	vSphereConnection.getServerConnection().logout();
	log.info("VM has the IP " + ip + " and the name " + vmName);
	return new VMInstance(vmName, ip);

    }

    private void configureVM(ServiceInstance vSphereConnection, VirtualMachine vm, String vmUsername,
	String vmUserPassword, String vmPublicKey)
	    throws GuestOperationsFault, InvalidState, TaskInProgress, FileFault, RuntimeFault, RemoteException {

	// a "hello world" with the command execution, to test, if the command
	// execution works
	// executeCommand(vSphereConnection, vm, vmUsername, vmUserPassword,
	// "echo -e \"yeah\" > itworks2;");

	// create the user with password
	executeCommand(vSphereConnection, vm, vmUsername, vmUserPassword,
	    "if ! id \"" + vmUsername + "\" >/dev/null 2>&1; then \n" + "useradd -D " + vmUsername + "\n" + "fi\n"
		+ "echo -e \"#!/bin/bash \necho \\\"" + vmUsername + ":" + vmUserPassword
		+ "\\\" | sudo chpasswd\" > change.sh \n" + "echo \"ubuntu\" | sudo -S sh change.sh \n"
		+ "rm change.sh");

	// change sudo group so that the given user can sudo without typing the
	// password
	// sed -i '/%sudo/c\%sudo ALL=(ALL:ALL) NOPASSWD:ALL'

	String replacementString = "%sudo ALL=(ALL:ALL) NOPASSWD:ALL";
	String replacedString = "%sudo";
	executeCommand(vSphereConnection, vm, vmUsername, vmUserPassword,
	    "echo \"ubuntu\" | sudo -S sed -i '/" + replacedString + "/c\\" + replacementString + "' /etc/sudoers;");

	// inject the public key
	executeCommand(vSphereConnection, vm, vmUsername, vmUserPassword,
	    "echo -e \"" + vmPublicKey + "\" > ~/.ssh/authorized_keys;\n");
    }

    private void executeCommand(ServiceInstance serviceInstance, VirtualMachine vm, String vmUsername,
	String vmUserPassword, String bashCommand)
	    throws GuestOperationsFault, InvalidState, TaskInProgress, FileFault, RuntimeFault, RemoteException {

	GuestOperationsManager gom = serviceInstance.getGuestOperationsManager();

	NamePasswordAuthentication npa = new NamePasswordAuthentication();
	npa.username = "ubuntu";
	npa.password = "ubuntu";

	GuestProgramSpec spec = new GuestProgramSpec();
	spec.programPath = "/";
	spec.arguments = "#!/bin/bash \n" + bashCommand;// + " >>
	// ~./management.log";
	log.info("Invoke script on provisioned system: \n" + spec.getArguments());

	GuestProcessManager gpm = gom.getProcessManager(vm);

	try {
	    long pid = gpm.startProgramInGuest(npa, spec);
	} catch (Exception e) {
	    log.warning(e.getLocalizedMessage());
	    // "Standard username and password does not work (ubuntu/ubuntu),
	    // thus, try with the credentials from the parameters");
	    try {

		NamePasswordAuthentication npa2 = new NamePasswordAuthentication();
		npa.username = vmUsername;
		npa.password = vmUserPassword;

		long pid = gpm.startProgramInGuest(npa2, spec);

	    } catch (Exception e2) {
		log.severe(e2.getLocalizedMessage());
	    }
	} finally {

	}
    }

    /**
     * Method for terminating a VM, if the VM exists.
     * 
     * @param apiAddress
     * @param apiUser
     * @param apiPassword
     * @param vsBasePath
     * @param vmName
     * @throws InvalidProperty
     * @throws RuntimeFault
     * @throws RemoteException
     * @throws MalformedURLException
     * @throws CredentialsFormatException
     * @throws InterruptedException
     */
    public void terminate(String apiAddress, String apiUser, String apiPassword, String vsBasePath, String vmName)
	throws InvalidProperty, RuntimeFault, RemoteException, MalformedURLException, InterruptedException,
	CredentialsFormatException {

	log.info("Terminate the VM: " + vmName);

	ServiceInstance si = connectToVSphere(apiAddress, apiUser, apiPassword);

	log.info("Searching root folder.");
	Folder rootFolder = si.getRootFolder();

	Datacenter dc = getDataCenter(si, rootFolder);
	Folder vmFolder = getVMFolder(si, dc, vsBasePath);

	if (!terminateIfExists(vsBasePath, vmName, si, vmFolder)) {
	    log.warning("The VM \"" + vmName + "\" did not exist, but should have to!");
	}
	si.getServerConnection().logout();
    }

    private void waitForGuestTools(ServiceInstance vSphereConnection, VirtualMachine vm) throws InterruptedException {
	log.info("Entering wait loop until either: (1) the guest tools are available or (2) a timeout of "
	    + waitTime / 1000 + " seconds has passed.");
	boolean run = true;
	long millis = System.currentTimeMillis();
	while (run) {

	    Thread.sleep(1000);

	    if ("guestToolsRunning".equals(vm.getGuest().toolsRunningStatus)) {
		log.info("Guest tools are available.");
		run = false;
	    } else if (System.currentTimeMillis() - millis > waitTime) {
		log.severe("Wait time exceeded, logging out and stopping the creation of the VM prematurely.");
		run = false;
		vSphereConnection.getServerConnection().logout();
	    }

	}
	log.info("Stopped waiting.");
    }

    private static Folder searchFolder(ManagedEntity[] folders, String searchTarget) throws RemoteException {
	Folder vmFolder = null;
	for (ManagedEntity folder : folders) {
	    // log.info(folder.getClass().getName());
	    if (folder.getName().equals(searchTarget)) {
		vmFolder = (Folder) folder;
		break;
	    } else {
		if (folder.getClass() == com.vmware.vim25.mo.Folder.class) {
		    vmFolder = searchFolder(((Folder) folder).getChildEntity(), searchTarget);
		}
		if (vmFolder != null) {
		    break;
		}
	    }
	}
	return vmFolder;
    }

    private static VirtualMachine searchVM(ManagedEntity[] folders, String searchTarget) throws RemoteException {
	VirtualMachine vm = null;
	for (ManagedEntity folder : folders) {
	    if (folder.getName().equals(searchTarget)) {
		vm = (VirtualMachine) folder;
		break;
	    } else {
		if (folder.getClass() == com.vmware.vim25.mo.Folder.class) {
		    vm = searchVM(((Folder) folder).getChildEntity(), searchTarget);
		}
		if (vm != null) {
		    break;
		}
	    }
	}
	return vm;
    }

    private VirtualMachine cloneVM(String vsBasePath, String vMName, ServiceInstance si, Folder vmFolder,
	ResourcePool rp, VirtualMachine template)
	    throws VmConfigFault, TaskInProgress, CustomizationFault, FileFault, InvalidState, InsufficientResourcesFault,
	    MigrationFault, InvalidDatastore, RuntimeFault, RemoteException, InterruptedException, InvalidProperty {
	log.info("Cloning VM...");
	VirtualMachineCloneSpec cloneSpec = new VirtualMachineCloneSpec();
	VirtualMachineRelocateSpec vmrs = new VirtualMachineRelocateSpec();
	vmrs.setPool(rp.getMOR());
	cloneSpec.setLocation(vmrs);
	cloneSpec.setPowerOn(true);
	cloneSpec.setTemplate(false);

	Task task = template.cloneVM_Task((Folder) template.getParent(), vMName, cloneSpec);

	if (task.waitForTask() == Task.SUCCESS) {
	    log.info("Succesfully cloned VM template to '" + vsBasePath + "/" + vMName + "'.");
	} else {
	    log.severe("Could not clone VM template to'" + vsBasePath + "/" + vMName + "'. Error: "
		+ task.getTaskInfo().getError().getLocalizedMessage());
	    si.getServerConnection().logout();
	    // System.exit(-1);
	}

	VirtualMachine vm = searchVM(vmFolder.getChildEntity(), vMName);
	return vm;
    }

    private boolean terminateIfExists(String vsBasePath, String vMName, ServiceInstance si, Folder vmFolder)
	throws RemoteException, InvalidProperty, RuntimeFault, TaskInProgress, InvalidState, InterruptedException,
	VimFault {

	log.info("Checking if VM \"" + vMName + "\" already exists.");
	VirtualMachine vm = searchVM(vmFolder.getChildEntity(), vMName);

	if (vm != null) {

	    log.info("VM '" + vsBasePath + "/" + vMName + "' already exists. Trying to delete...");

	    // stop the VM
	    if (vm.getRuntime().getPowerState() != VirtualMachinePowerState.poweredOff) {
		log.info("VM '" + vsBasePath + "/" + vMName + "' is running. Trying to stop...");
		Task task = vm.powerOffVM_Task();
		if (task.waitForTask() == Task.SUCCESS) {
		    log.info("Succesfully stopped VM '" + vsBasePath + "/" + vMName + "'.");
		} else {
		    log.severe("Could not stop VM '" + vsBasePath + "/" + vMName + "'.");
		    return false;
		}
	    }

	    // delete the VM
	    Task task = vm.destroy_Task();
	    if (task.waitForTask() == Task.SUCCESS) {
		log.info("Succesfully destroyed VM '" + vsBasePath + "/" + vMName + "'.");
		return true;
	    } else {
		log.severe("Could not destroy VM '" + vsBasePath + "/" + vMName + "'.");
		return false;
	    }
	} else {
	    log.info("VM \"" + vMName + "\" did not exist.");
	    return true;
	}
    }

    private VirtualMachine getVMTemplate(String templateID, ServiceInstance si, Datacenter dc)
	throws RemoteException, InvalidProperty, RuntimeFault {
	log.info("Searching VM Template.");
	VirtualMachine template = searchVM(dc.getVmFolder().getChildEntity(), templateID);
	if (template == null) {
	    log.severe("VM template does not exist: '" + templateID + "'.");
	    si.getServerConnection().logout();
	    // System.exit(-1);
	} else {
	    log.info("Found template " + template.getName());
	}
	return template;
    }

    private ResourcePool getResourcePool(String vsResourcePool, ServiceInstance si, Folder rootFolder)
	throws InvalidProperty, RuntimeFault, RemoteException {
	log.info("Searching resource pool.");
	ResourcePool rp = (ResourcePool) new InventoryNavigator(rootFolder).searchManagedEntity("ResourcePool",
	    vsResourcePool);
	if (rp == null) {
	    log.severe("Could not find '" + vsResourcePool + "' as resource pool.");
	    si.getServerConnection().logout();
	    // System.exit(-1);
	} else {
	    log.info("Found resource pool " + vsResourcePool);
	}
	return rp;
    }

    private Folder getVMFolder(ServiceInstance si, Datacenter dc, String vsBasePath)
	throws RemoteException, InvalidProperty, RuntimeFault {
	log.info("Searching base path \"" + vsBasePath + "\".");
	String[] pathArray = vsBasePath.split("/");
	Folder vmFolder = searchFolder(dc.getVmFolder().getChildEntity(), pathArray[pathArray.length - 1]);
	if (vmFolder == null) {
	    log.severe("Could not find '" + vsBasePath + "' as base path.");
	    si.getServerConnection().logout();
	    // System.exit(-1);
	}
	return vmFolder;
    }

    private Datacenter getDataCenter(ServiceInstance si, Folder rootFolder)
	throws InvalidProperty, RuntimeFault, RemoteException {
	log.info("Searching data center.");
	ManagedEntity[] elements = new InventoryNavigator(rootFolder).searchManagedEntities("Datacenter");
	if (elements.length < 1) {
	    log.severe("Could not find a datacenter.");
	    si.getServerConnection().logout();
	    // System.exit(-1);
	} else if (elements.length > 1) {
	    log.warning("Found " + String.valueOf(elements.length) + " datacenters, using first one: '"
		+ elements[0].getName() + "'.");
	}
	Datacenter dc = (Datacenter) elements[0];
	// log.info("Found Datastore " + dc.getName());
	return dc;
    }

    private ServiceInstance connectToVSphere(String regionEndpoint, String user, String secretKey)
	throws RemoteException, MalformedURLException, CredentialsFormatException {
	log.info("Connecting to " + regionEndpoint);
	ServiceInstance si = new ServiceInstance(new URL(regionEndpoint.trim()), user.trim(), secretKey.trim(), true);
	// if(null == si){
	// throw new CredentialsFormatException("Could not connect or
	// authenticate to the vSphere server.");
	// }
	return si;
    }
}
