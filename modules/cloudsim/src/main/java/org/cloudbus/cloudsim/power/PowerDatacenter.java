/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.power;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.predicates.PredicateType;

/**
 * PowerDatacenter is a class that enables simulation of power-aware data centers.
 * <p>
 * <br/>If you are using any algorithms, policies or workload included in the power package please cite
 * the following paper:<br/>
 *
 * <ul>
 * <li><a href="http://dx.doi.org/10.1002/cpe.1867">Anton Beloglazov, and Rajkumar Buyya, "Optimal Online Deterministic Algorithms and Adaptive
 * Heuristics for Energy and Performance Efficient Dynamic Consolidation of Virtual Machines in
 * Cloud Data Centers", Concurrency and Computation: Practice and Experience (CCPE), Volume 24,
 * Issue 13, Pages: 1397-1420, John Wiley & Sons, Ltd, New York, USA, 2012</a>
 * </ul>
 *
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 2.0
 */
public class PowerDatacenter extends Datacenter {

    /**
     * The datacenter consumed power.
     */
    private double power;

    /**
     * Indicates if migrations are disabled or not.
     */
    private boolean disableMigrations;

    /**
     * The last time submitted cloudlets were processed.
     */
    private double cloudletSubmitted;

    /**
     * The VM migration count.
     */
    private int migrationCount;

    /**
     * Instantiates a new PowerDatacenter.
     *
     * @param name               the datacenter name
     * @param characteristics    the datacenter characteristics
     * @param schedulingInterval the scheduling interval
     * @param vmAllocationPolicy the vm provisioner
     * @param storageList        the storage list
     * @throws Exception the exception
     */
    public PowerDatacenter(
            String name,
            DatacenterCharacteristics characteristics,
            VmAllocationPolicy vmAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);

        setPower(0.0);
        setDisableMigrations(false);
        setCloudletSubmitted(-1);
        setMigrationCount(0);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.VM_INITIATE_PRE_COPY_MIGRATION:
                processInitiateVmPreCopyMigration(ev, false);
                break;

            case CloudSimTags.VM_CONFIRM_PRE_COPY_MIGRATION_INITIATION:
                processConfirmVmPreCopyMigrationInitiation(ev, false);
                break;

            case CloudSimTags.VM_FINALIZE_PRE_COPY_MIGRATION:
                processFinalizeVmPreCopyMigration(ev, false);
                break;
        }
        super.processEvent(ev);
    }

    private void processInitiateVmPreCopyMigration(SimEvent ev, boolean ack) {
        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }


        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;

        PowerVm vmOriginal = (PowerVm) migrate.get("vm");
        PowerVm vm = (PowerVm) vmOriginal.clone();
        Host host = (Host) migrate.get("host");

        Log.printLine(CloudSim.clock() + "\" : START - PRE-COPY MIGRATING VM " + vmOriginal.getId() + " from host " + vmOriginal.getHost().getId() +
                " to host " + host.getId() + ". Reserving resources and creating VM.");


        boolean result = getVmAllocationPolicy().allocateHostForVm(vm, host);
        if (!result) {
            Log.printLine("[Datacenter.processVmMigrate] VM allocation to the destination host failed");
            System.exit(0);
        }


        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = vm.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(ev.getSource(), CloudSimTags.VM_CREATE_ACK, data);
        }


        Log.printLine("SEND MIGRATION CONFIRMATION WITH TIME OFFSET of VM " + vmOriginal.getId() + " to host " + vmOriginal.getHost().getId());


        Map<String, Object> migrationElements = new HashMap<String, Object>();
        migrationElements.put("host", host);
        migrationElements.put("vmOriginal", vmOriginal);
        migrationElements.put("vmCopy", vm);
        send(
                getId(),
                vm.getRam() / ((double) host.getBw() / (2 * 8000)),
                CloudSimTags.VM_CONFIRM_PRE_COPY_MIGRATION_INITIATION,
                migrationElements);


        Log.formatLine(
                "%.2f: Pre-copy migration Initialization of VM #%d to Host #%d is completed",
                CloudSim.clock(),
                vmOriginal.getId(),
                host.getId());
    }

    private void processConfirmVmPreCopyMigrationInitiation(SimEvent ev, boolean b) {
        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }


        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;


        PowerVm vm = (PowerVm) migrate.get("vmOriginal");
        Host host = vm.getHost();

        Log.printLine("Deallocating VM on source host " + host.getId() + " before final migration. vmId = " + vm.getId());
        //TODO jak wstrzymać VM? host.vmDestroy(vm);?

        getVmAllocationPolicy().deallocateHostForVm(vm);
        host.vmDestroy(vm);
        host.removeMigratingInVm(vm);


        Log.printLine("Sending finalize pre-copy migration request. vmId = " + vm.getId());

        send(
                getId(),
                0,
                CloudSimTags.VM_FINALIZE_PRE_COPY_MIGRATION,
                tmp);

    }

    private void processFinalizeVmPreCopyMigration(SimEvent ev, boolean b) {

        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }


        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;

        Vm vm = (Vm) migrate.get("vmCopy");
        Host host = (Host) migrate.get("host");
        if(vm.getId() == 8){
            int x = 0;
        }


        //transfer state to destination server (CPU, registers, VM memory)
        host.transferVmState(vm);

    }

    //	private List<Map<String, Object>> postprocessMigrationMap(List<Map<String, Object>> migrationMap){
//		List<Map<String, Object>> newMigrationMap = new ArrayList<Map<String, Object>>();
//		for(Map<String, Object> item: migrationMap){
//			Map<String, Object> element = new HashMap<String, Object>();
//			Vm vm = (Vm) item.get("vm");
//			Host destinationHost = (Host) item.get("host");
//			element.put("vm", vm);
//			element.put("host",destinationHost);
//			element.put("vmCopy", vm.getHost());
//			element.put("vmSource", vm1);
//			element.put("migrationSourceHost", vm1.getHost());
//			element.put("migrationDestinationHost", datacenter0.getHostList().get(1));
//
//			newMigrationMap.add(element);
//		}
//
//	}
    @Override
    protected void updateCloudletProcessing() {
        if (getCloudletSubmitted() == -1 || getCloudletSubmitted() == CloudSim.clock()) {
            CloudSim.cancelAll(getId(), new PredicateType(CloudSimTags.VM_DATACENTER_EVENT));
            schedule(getId(), getSchedulingInterval(), CloudSimTags.VM_DATACENTER_EVENT);
            return;
        }
        double currentTime = CloudSim.clock();

        // if some time passed since last processing
        if (currentTime > getLastProcessTime()) {
            System.out.print(currentTime + " ");

            double minTime = updateCloudetProcessingWithoutSchedulingFutureEventsForce();

            if (!isDisableMigrations()) {
                List<Map<String, Object>> migrationMap = getVmAllocationPolicy().optimizeAllocation(
                        getVmList());

                if (migrationMap != null) {
                    for (Map<String, Object> migrate : migrationMap) {
                        Vm vm = (Vm) migrate.get("vm");
                        PowerHost targetHost = (PowerHost) migrate.get("host");
                        PowerHost oldHost = (PowerHost) vm.getHost();

                        if (oldHost == null) {
                            Log.formatLine(
                                    "%.2f: Migration of VM #%d to Host #%d is started",
                                    currentTime,
                                    vm.getId(),
                                    targetHost.getId());
                        } else {
                            Log.formatLine(
                                    "%.2f: Migration of VM #%d from Host #%d to Host #%d is started",
                                    currentTime,
                                    vm.getId(),
                                    oldHost.getId(),
                                    targetHost.getId());
                        }

                        targetHost.addMigratingInVm(vm);
                        incrementMigrationCount();

                        /** VM migration delay = RAM / bandwidth **/
                        // we use BW / 2 to model BW available for migration purposes, the other
                        // half of BW is for VM communication
                        // around 16 seconds for 1024 MB using 1 Gbit/s network
                        send(
                                getId(),
                                vm.getRam() / ((double) targetHost.getBw() / (2 * 8000)),
                                CloudSimTags.VM_INITIATE_PRE_COPY_MIGRATION,
                                migrate);
                    }
                }
            }

            // schedules an event to the next time
            if (minTime != Double.MAX_VALUE) {
                CloudSim.cancelAll(getId(), new PredicateType(CloudSimTags.VM_DATACENTER_EVENT));
                send(getId(), getSchedulingInterval(), CloudSimTags.VM_DATACENTER_EVENT);
            }

            setLastProcessTime(currentTime);
        }
    }


    /**
     * Update cloudet processing without scheduling future events.
     *
     * @return the double
     * @todo There is an inconsistence in the return value of this
     * method with return value of similar methods
     * such as {@link #updateCloudetProcessingWithoutSchedulingFutureEventsForce()},
     * that returns {@link Double#MAX_VALUE} by default.
     * The current method returns 0 by default.
     * @see #updateCloudetProcessingWithoutSchedulingFutureEventsForce()
     */
    protected double updateCloudetProcessingWithoutSchedulingFutureEvents() {
        if (CloudSim.clock() > getLastProcessTime()) {
            return updateCloudetProcessingWithoutSchedulingFutureEventsForce();
        }
        return 0;
    }

    /**
     * Update cloudet processing without scheduling future events.
     *
     * @return expected time of completion of the next cloudlet in all VMs of all hosts or
     * {@link Double#MAX_VALUE} if there is no future events expected in this host
     */
    protected double updateCloudetProcessingWithoutSchedulingFutureEventsForce() {
        double currentTime = CloudSim.clock();
        double minTime = Double.MAX_VALUE;
        double timeDiff = currentTime - getLastProcessTime();
        double timeFrameDatacenterEnergy = 0.0;

        Log.printLine("\n\n--------------------------------------------------------------\n\n");
        Log.formatLine("New resource usage for the time frame starting at %.2f:", currentTime);

        for (PowerHost host : this.<PowerHost>getHostList()) {
            Log.printLine();

            double time = host.updateVmsProcessing(currentTime); // inform VMs to update processing
            if (time < minTime) {
                minTime = time;
            }

            Log.formatLine(
                    "%.2f: [Host #%d] utilization is %.2f%%",
                    currentTime,
                    host.getId(),
                    host.getUtilizationOfCpu() * 100);
        }

        if (timeDiff > 0) {
            Log.formatLine(
                    "\nEnergy consumption for the last time frame from %.2f to %.2f:",
                    getLastProcessTime(),
                    currentTime);

            for (PowerHost host : this.<PowerHost>getHostList()) {
                double previousUtilizationOfCpu = host.getPreviousUtilizationOfCpu();
                double utilizationOfCpu = host.getUtilizationOfCpu();
                double timeFrameHostEnergy = host.getEnergyLinearInterpolation(
                        previousUtilizationOfCpu,
                        utilizationOfCpu,
                        timeDiff);
                timeFrameDatacenterEnergy += timeFrameHostEnergy;

                Log.printLine();
                Log.formatLine(
                        "%.2f: [Host #%d] utilization at %.2f was %.2f%%, now is %.2f%%",
                        currentTime,
                        host.getId(),
                        getLastProcessTime(),
                        previousUtilizationOfCpu * 100,
                        utilizationOfCpu * 100);
                Log.formatLine(
                        "%.2f: [Host #%d] energy is %.2f W*sec",
                        currentTime,
                        host.getId(),
                        timeFrameHostEnergy);
            }

            Log.formatLine(
                    "\n%.2f: Data center's energy is %.2f W*sec\n",
                    currentTime,
                    timeFrameDatacenterEnergy);
        }

        setPower(getPower() + timeFrameDatacenterEnergy);

        checkCloudletCompletion();

        /** Remove completed VMs **/
        for (PowerHost host : this.<PowerHost>getHostList()) {
            for (Vm vm : host.getCompletedVms()) {
                getVmAllocationPolicy().deallocateHostForVm(vm);
                getVmList().remove(vm);
                Log.printLine("VM #" + vm.getId() + " has been deallocated from host #" + host.getId());
            }
        }

        Log.printLine();

        setLastProcessTime(currentTime);
        return minTime;
    }

    @Override
    protected void processVmMigrate(SimEvent ev, boolean ack) {
        updateCloudetProcessingWithoutSchedulingFutureEvents();
        super.processVmMigrate(ev, ack);
        SimEvent event = CloudSim.findFirstDeferred(getId(), new PredicateType(CloudSimTags.VM_MIGRATE));
        if (event == null || event.eventTime() > CloudSim.clock()) {
            updateCloudetProcessingWithoutSchedulingFutureEventsForce();
        }
    }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        super.processCloudletSubmit(ev, ack);
        setCloudletSubmitted(CloudSim.clock());
    }

    /**
     * Gets the power.
     *
     * @return the power
     */
    public double getPower() {
        return power;
    }

    /**
     * Sets the power.
     *
     * @param power the new power
     */
    protected void setPower(double power) {
        this.power = power;
    }

    /**
     * Checks if PowerDatacenter is in migration.
     *
     * @return true, if PowerDatacenter is in migration; false otherwise
     */
    protected boolean isInMigration() {
        boolean result = false;
        for (Vm vm : getVmList()) {
            if (vm.isInMigration()) {
                result = true;
                break;
            }
        }
        return result;
    }

    /**
     * Checks if migrations are disabled.
     *
     * @return true, if  migrations are disable; false otherwise
     */
    public boolean isDisableMigrations() {
        return disableMigrations;
    }

    /**
     * Disable or enable migrations.
     *
     * @param disableMigrations true to disable migrations; false to enable
     */
    public void setDisableMigrations(boolean disableMigrations) {
        this.disableMigrations = disableMigrations;
    }

    /**
     * Checks if is cloudlet submited.
     *
     * @return true, if is cloudlet submited
     */
    protected double getCloudletSubmitted() {
        return cloudletSubmitted;
    }

    /**
     * Sets the cloudlet submitted.
     *
     * @param cloudletSubmitted the new cloudlet submited
     */
    protected void setCloudletSubmitted(double cloudletSubmitted) {
        this.cloudletSubmitted = cloudletSubmitted;
    }

    /**
     * Gets the migration count.
     *
     * @return the migration count
     */
    public int getMigrationCount() {
        return migrationCount;
    }

    /**
     * Sets the migration count.
     *
     * @param migrationCount the new migration count
     */
    protected void setMigrationCount(int migrationCount) {
        this.migrationCount = migrationCount;
    }

    /**
     * Increment migration count.
     */
    protected void incrementMigrationCount() {
        setMigrationCount(getMigrationCount() + 1);
    }

}
