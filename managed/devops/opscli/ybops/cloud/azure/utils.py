# Copyright 2020 YugaByte, Inc. and Contributors
#
# Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# https://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import DiskCreateOption
from azure.mgmt.privatedns import PrivateDnsManagementClient
from msrestazure.azure_exceptions import CloudError
from ybops.utils import DNS_RECORD_SET_TTL, MIN_MEM_SIZE_GB, \
    MIN_NUM_CORES
from ybops.utils.ssh import format_rsa_key, validated_key_file
from threading import Thread

import adal
import base64
import datetime
import json
import logging
import os
import re
import requests
import yaml

SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID")
RESOURCE_GROUP = os.environ.get("AZURE_RG")

NETWORK_PROVIDER_BASE_PATH = "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network"
SUBNET_ID_FORMAT_STRING = NETWORK_PROVIDER_BASE_PATH + "/virtualNetworks/{}/subnets/{}"
NSG_ID_FORMAT_STRING = NETWORK_PROVIDER_BASE_PATH + "/networkSecurityGroups/{}"
ULTRASSD_LRS = "ultrassd_lrs"
VNET_ID_FORMAT_STRING = NETWORK_PROVIDER_BASE_PATH + "/virtualNetworks/{}"
AZURE_SKU_FORMAT = {"premium_lrs": "Premium_LRS",
                    "standardssd_lrs": "StandardSSD_LRS",
                    ULTRASSD_LRS: "UltraSSD_LRS"}
YUGABYTE_VNET_PREFIX = "yugabyte-vnet-{}"
YUGABYTE_SUBNET_PREFIX = "yugabyte-subnet-{}"
YUGABYTE_SG_PREFIX = "yugabyte-sg-{}"
YUGABYTE_PEERING_FORMAT = "yugabyte-peering-{}-{}"
RESOURCE_SKU_URL = "https://management.azure.com/subscriptions/{}/providers" \
    "/Microsoft.Compute/skus".format(SUBSCRIPTION_ID)
GALLERY_IMAGE_ID_REGEX = re.compile(
    "/subscriptions/(?P<subscription_id>[^/]*)/resourceGroups"
    "/(?P<resource_group>[^/]*)/providers/Microsoft.Compute/galleries/(?P<gallery_name>[^/]*)"
    "/images/(?P<image_definition_name>[^/]*)/versions/(?P<version_id>[^/]*)")
VM_PRICING_URL_FORMAT = "https://prices.azure.com/api/retail/prices?$filter=" \
    "serviceFamily eq 'Compute' " \
    "and serviceName eq 'Virtual Machines' and priceType eq 'Consumption' " \
    "and armRegionName eq '{}'"
PRIVATE_DNS_ZONE_ID_REGEX = re.compile(
    "/subscriptions/(?P<subscription_id>[^/]*)/resourceGroups/(?P<resource_group>[^/]*)"
    "/providers/Microsoft.Network/privateDnsZones/(?P<zone_name>[^/]*)")
CLOUDINIT_EPHEMERAL_MNTPOINT = {
    "mounts": [
        ["ephemeral0", "/mnt/resource"]
    ]
}


class GetPriceWorker(Thread):
    def __init__(self, region):
        Thread.__init__(self)
        self.region = region
        self.vm_name_to_price_dict = {}

    def run(self):
        url = VM_PRICING_URL_FORMAT.format(self.region)
        while url:
            try:
                price_info = requests.get(url).json()
            except Exception as e:
                logging.error("Error getting price information for region {}: {}"
                              .format(self.region, str(e)))
                break

            for info in price_info.get('Items'):
                # Azure API doesn't support regex as of 3/08/2021, so manually parse out Windows.
                # Some VMs also show $0.0 as the price for some reason, so ignore those as well.
                if not (info['productName'].endswith(' Windows') or info['unitPrice'] == 0):
                    self.vm_name_to_price_dict[info['armSkuName']] = info['unitPrice']
            url = price_info.get('NextPageLink')


def get_credentials():
    credentials = ServicePrincipalCredentials(
        client_id=os.environ.get("AZURE_CLIENT_ID"),
        secret=os.environ.get("AZURE_CLIENT_SECRET"),
        tenant=os.environ.get("AZURE_TENANT_ID")
    )
    return credentials


def create_resource_group(region, subscription_id=None, resource_group=None):
    rg = resource_group if resource_group else RESOURCE_GROUP
    sid = subscription_id if subscription_id else SUBSCRIPTION_ID
    resource_group_client = ResourceManagementClient(get_credentials(), sid)
    if resource_group_client.resource_groups.check_existence(rg):
        return
    resource_group_params = {'location': region}
    return resource_group_client.resource_groups.create_or_update(rg, resource_group_params)


def id_to_name(resourceId):
    return str(resourceId.split('/')[-1])


def get_zones(region, metadata):
    return ["{}-{}".format(region, zone)
            for zone in metadata["regions"].get(region, {}).get("zones", [])]


def cloud_init_encoded(**kwargs):
    """
    Create base64 encoded cloud init data.

    **kwargs are additional key/values to add to the cloud init
    """
    ci_header = "#cloud-config"
    cloud_init = CLOUDINIT_EPHEMERAL_MNTPOINT.copy()

    # Handle additional mounts. Allow overriding of ephemeral0 to /mnt/resource.
    # If ephemeral0 is provided in kwargs, we want to use the user defined mount point over what
    # we specify as the default. We will loop through all 'additional mounts', looking for
    # ephemeral0. If it is found, we want to override our default (which only includes an option
    # for ephemeral0). If ephemeral0 is not found in additional mounts, we want our ephemeral0
    # default + the other user provided mount points.
    # Remember, mounts is a list of lists - [ [ "ephemeral0", "/mnt/resource"] ]
    additional_mounts = kwargs.pop("mounts", [])
    for am in additional_mounts:
        # ephemeral and ephemeral0 refer to the same mount point, either may be used.
        if am[0] == "ephemeral" or am[0] == "ephemeral0":
            cloud_init["mounts"] = additional_mounts
            break
    else:
        cloud_init["mounts"].extend(additional_mounts)

    cloud_init.update(**kwargs)
    ci_data = yaml.dump(cloud_init)
    logging.debug("created cloud init data: {}".format(ci_data))

    lines = [
        ci_header,
        ci_data
    ]
    cloud_file = '\n'.join(lines)
    return base64.b64encode(cloud_file.encode('utf-8')).decode('utf-8')


class AzureBootstrapClient():
    def __init__(self, region_meta, network, metadata):
        self.credentials = get_credentials()
        self.network_client = network
        self.region_meta = region_meta
        self.metadata = metadata

    def create_default_vnet(self, cidr, region):

        vnet_params = {
            'location': region,
            'address_space': {
                'address_prefixes': [cidr]
            },
        }
        logging.debug("Creating Virtual Network {} with CIDR {}".format(
            YUGABYTE_VNET_PREFIX.format(region), cidr))
        creation_result = self.network_client.virtual_networks.create_or_update(
            RESOURCE_GROUP,
            YUGABYTE_VNET_PREFIX.format(region),
            vnet_params
        )
        return creation_result.result().name

    def create_default_subnet(self, vNet, cidr, region):
        subnet_params = {
            'address_prefix': cidr
        }
        logging.debug("Creating Subnet {} with CIDR {}".format(
            YUGABYTE_SUBNET_PREFIX.format(region),
            cidr
        ))
        creation_result = self.network_client.subnets.create_or_update(
            RESOURCE_GROUP,
            vNet,
            YUGABYTE_SUBNET_PREFIX.format(region),
            subnet_params
        )

        return creation_result.result().name

    def get_default_vnet(self, region):
        vnets = [resource.serialize() for resource in
                 self.network_client.virtual_networks.list(RESOURCE_GROUP)]
        for vnetJson in vnets:
            # parse vnet from ID
            vnetName = id_to_name(vnetJson.get("id"))
            if (vnetJson.get("location") == region and
                    vnetName.startswith(YUGABYTE_VNET_PREFIX.format(''))):
                logging.debug("Found default vnet {}".format(vnetName))
                return vnetName
        logging.info("Could not find default {} in region {}".format(YUGABYTE_VNET_PREFIX, region))
        return None

    def get_default_subnet(self, vnet):
        """
        vnet - name of the vnet in which to look for subnets
        """
        subnets = [resource.serialize() for resource in
                   self.network_client.subnets.list(RESOURCE_GROUP, vnet)]
        for subnet in subnets:
            # Maybe change to tags rather than filtering on name prefix
            if subnet.get("name").startswith(YUGABYTE_SUBNET_PREFIX.format('')):
                logging.debug("Found default subnet {}".format(subnet.get("name")))
                return subnet.get("name")
        logging.info("Could not find default {} in vnet {}".format(YUGABYTE_SUBNET_PREFIX, vnet))
        return None

    def get_default_sg(self, region):
        """
        This method is currently not used. Auto creation currently does not set up
        a default security groups so all VMs brought up by default will block
        all public Internt access.
        """
        sgs = [resource.serialize() for resource in
               self.network_client.network_security_groups.list(RESOURCE_GROUP)]
        for sg in sgs:
            if (sg.get("location") == region and
                    sg.get("name").startswith(YUGABYTE_SG_PREFIX.format(''))):
                return sg.get("name")
        logging.info("Could not find default {} in region {}".format(YUGABYTE_SG_PREFIX, region))
        return None

    def get_vnet_cidr(self, region):
        return self.metadata["region_cidr_format"].format(
            self.metadata["regions"][region]["cidr_prefix"])

    def get_subnet_cidr(self, region):
        return self.metadata["zone_cidr_format"].format(
            self.metadata["regions"][region]["cidr_prefix"], 16)

    def bootstrap(self, region):
        result = {}
        vnet = self.get_default_vnet(region)
        if not vnet:
            vnet = self.create_default_vnet(self.get_vnet_cidr(region), region)
        subnet = self.get_default_subnet(vnet)
        if not subnet:
            subnet = self.create_default_subnet(vnet, self.get_subnet_cidr(region), region)
        self.fill_metadata(region, vnet, subnet)
        return self

    def fill_metadata(self, region, vnet, subnet):
        region_meta = {}
        region_meta["vpcId"] = vnet
        zones = get_zones(region, self.metadata)
        azToSubnet = {zone: subnet for zone in zones}
        region_meta["azToSubnetIds"] = azToSubnet
        self.region_meta = region_meta

    def cleanup(self, region):
        vnet = self.get_default_vnet(region)
        if not vnet:
            logging.debug("Could not find default vnet in region {}".format(region))
            return
        subnet = self.get_default_subnet(vnet)
        if subnet:
            self.network_client.subnets.delete(RESOURCE_GROUP, vnet, subnet).result()
            logging.debug("Successfully deleted subnet {}".format(subnet))
        self.network_client.virtual_networks.delete(RESOURCE_GROUP, vnet).result()
        logging.debug("Successfully deleted vnet {}".format(vnet))

    def get_vnet_id(self, vnet):
        """
        Generate vnet id format from vnet name
        """
        return VNET_ID_FORMAT_STRING.format(SUBSCRIPTION_ID, RESOURCE_GROUP, vnet)

    def gen_peering_params(self, remote_region, remote_vnet):
        peering_params = {
            "remoteVirtualNetwork": {
                "id": self.get_vnet_id(remote_vnet)
            },
            "allowVirtualNetworkAccess": True,
            "allowForwardedTraffic": True,
            "allowGatewayTransit": False,
            "useRemoteGateways": False,
            "remoteAddressSpace": {
                "addressPrefixes": [
                    self.get_vnet_cidr(remote_region)
                ]
            }
        }
        return peering_params

    def create_peering(self, region1, vnet1, region2, vnet2):
        """
        Creates two-way peering
        """
        try:
            self.network_client.virtual_network_peerings.get(
                RESOURCE_GROUP,
                vnet1,
                YUGABYTE_PEERING_FORMAT.format(region1, region2)
            )
            self.network_client.virtual_network_peerings.get(
                RESOURCE_GROUP,
                vnet2,
                YUGABYTE_PEERING_FORMAT.format(region2, region1)
            )
            logging.debug("Found peerings on Virtual Network {} and Virtual Network {}.".format(
                vnet1, vnet2
            ))
            return
        except CloudError:
            logging.info("Could not find peerings on either {} or {} in regions {} and {}.".format(
                vnet1, vnet2, region1, region2
            ))
            pass
        pp1 = self.gen_peering_params(region2, vnet2)
        pp2 = self.gen_peering_params(region1, vnet1)
        # Peer 2 to 1
        peer1 = self.network_client.virtual_network_peerings.create_or_update(
            RESOURCE_GROUP,
            vnet1,
            YUGABYTE_PEERING_FORMAT.format(region1, region2),
            pp1
        )
        # Peer 1 to 2
        peer2 = self.network_client.virtual_network_peerings.create_or_update(
            RESOURCE_GROUP,
            vnet2,
            YUGABYTE_PEERING_FORMAT.format(region2, region1),
            pp2
        )
        peer1.result()
        peer2.result()
        return

    def peer(self, components):
        region_and_vnet_tuples = [(r, c.get("vpc_id")) for r, c in components.items()]
        for i in range(len(region_and_vnet_tuples) - 1):
            i_region, i_vnet = region_and_vnet_tuples[i]
            for j in range(i + 1, len(region_and_vnet_tuples)):
                j_region, j_vnet = region_and_vnet_tuples[j]
                self.create_peering(i_region, i_vnet, j_region, j_vnet)
        return

    def to_components(self):
        reg_info = {}
        reg_info["vpc_id"] = self.region_meta.get("vpcId")
        sg = self.region_meta.get("customSecurityGroupId", None)
        if sg:
            reg_info["security_group"] = [{"id": sg, "name": sg}]
        reg_info["zones"] = self.region_meta.get("azToSubnetIds")
        return reg_info


class AzureCloudAdmin():
    def __init__(self, metadata):
        self.metadata = metadata
        self.credentials = get_credentials()
        self.compute_client = ComputeManagementClient(self.credentials, SUBSCRIPTION_ID)
        self.network_client = NetworkManagementClient(self.credentials, SUBSCRIPTION_ID)

        self.dns_client = None

    def network(self, per_region_meta={}):
        return AzureBootstrapClient(per_region_meta, self.network_client, self.metadata)

    def append_disk(self, vm, vm_name, disk_name, size, lun, zone, vol_type, region, tags,
                    disk_iops, disk_throughput):
        disk_params = {
            "location": region,
            "disk_size_gb": size,
            "creation_data": {
                "create_option": DiskCreateOption.empty
            },
            "sku": {
                "name": AZURE_SKU_FORMAT[vol_type]
            }
        }
        if zone is not None:
            disk_params["zones"] = [zone]
        if tags:
            disk_params["tags"] = tags

        if vol_type == ULTRASSD_LRS:
            if disk_iops is not None:
                disk_params['disk_iops_read_write'] = disk_iops
            if disk_throughput is not None:
                disk_params['disk_mbps_read_write'] = disk_throughput

        data_disk = self.compute_client.disks.create_or_update(
            RESOURCE_GROUP,
            disk_name,
            disk_params
        ).result()

        vm.storage_profile.data_disks.append({
            "lun": lun,
            "name": disk_name,
            "create_option": DiskCreateOption.attach,
            "managed_disk": {
                "storageAccountType": AZURE_SKU_FORMAT[vol_type],
                "id": data_disk.id
            }
        })

        async_disk_attach = self.compute_client.virtual_machines.create_or_update(
            RESOURCE_GROUP,
            vm_name,
            vm
        )

        async_disk_attach.wait()
        return async_disk_attach.result()

    def tag_disks(self, vm, tags):
        # Updating requires Disk as input rather than OSDisk. Retrieve Disk class with OSDisk name.
        disk = self.compute_client.disks.get(
            RESOURCE_GROUP,
            vm.storage_profile.os_disk.name
        )
        disk.tags = tags
        self.compute_client.disks.create_or_update(
            RESOURCE_GROUP,
            disk.name,
            disk
        )

        for disk in vm.storage_profile.data_disks:
            # The data disk returned from vm.storage_profile can't be deserialized properly.
            disk = self.compute_client.disks.get(
                RESOURCE_GROUP,
                disk.name
            )
            disk.tags = tags
            self.compute_client.disks.create_or_update(
                RESOURCE_GROUP,
                disk.name,
                disk
            )

    def get_public_ip_name(self, vm_name):
        return vm_name + '-IP'

    def get_nic_name(self, vm_name):
        return vm_name + '-NIC'

    def create_or_update_public_ip_address(self, vm_name, zone, region, tags):
        public_ip_addess_params = {
            "location": region,
            "sku": {
                "name": "Standard"  # Only standard SKU supports zone
            },
            "public_ip_allocation_method": "Static",
        }
        if zone is not None:
            public_ip_addess_params["zones"] = [zone]
        if tags:
            public_ip_addess_params["tags"] = tags

        creation_result = self.network_client.public_ip_addresses.create_or_update(
            RESOURCE_GROUP,
            self.get_public_ip_name(vm_name),
            public_ip_addess_params
        )
        return creation_result.result()

    def create_or_update_nic(self, vm_name, vnet, subnet, zone, nsg, region, public_ip, tags):
        """
        Creates network interface and returns the id of the resource for use in
        vm creation.
            vm_name, vnet, subnet - String representing name of resource
            public_ip - bool if public_ip should be assigned
        """
        nic_params = {
            "location": region,
            "ip_configurations": [{
                "name": vm_name + "-IPConfig",
                "subnet": {
                    "id": self.get_subnet_id(vnet, subnet)
                },
            }],
        }
        if public_ip:
            publicIPAddress = self.create_or_update_public_ip_address(vm_name, zone, region, tags)
            nic_params["ip_configurations"][0]["public_ip_address"] = publicIPAddress
        if nsg:
            nic_params['networkSecurityGroup'] = {'id': self.get_nsg_id(nsg)}
        if tags:
            nic_params['tags'] = tags
        creation_result = self.network_client.network_interfaces.create_or_update(
            RESOURCE_GROUP,
            self.get_nic_name(vm_name),
            nic_params
        )

        return creation_result.result().id

    # The method is idempotent. Any failure raises exception such that it can be retried.
    def destroy_orphaned_resources(self, vm_name, node_uuid):
        if not node_uuid or not vm_name:
            logging.error("[app] Params vm_name and node_uuid must be passed")
            return
        logging.info("[app] Destroying orphaned resources for {}".format(vm_name))

        disk_dels = {}
        # TODO: filter does not work.
        disk_filter_param = "substringof('{}', name)".format(vm_name)
        disk_list = self.compute_client.disks.list_by_resource_group(
            RESOURCE_GROUP, filter=disk_filter_param)
        if disk_list:
            for disk in disk_list:
                if (disk.name.startswith(vm_name) and disk.tags
                        and disk.tags.get('node-uuid') == node_uuid):
                    logging.info("[app] Deleting disk {}".format(disk.name))
                    disk_del = self.compute_client.disks.delete(RESOURCE_GROUP, disk.name)
                    disk_dels[disk.name] = disk_del

        nic_name = self.get_nic_name(vm_name)
        ip_name = self.get_public_ip_name(vm_name)
        try:
            nic_info = self.network_client.network_interfaces.get(RESOURCE_GROUP, nic_name)
            if nic_info.tags and nic_info.tags.get('node-uuid') == node_uuid:
                logging.info("[app] Deleted nic {}".format(nic_name))
                nic_del = self.network_client.network_interfaces.delete(RESOURCE_GROUP, nic_name)
                nic_del.wait()
                logging.info("[app] Deleted nic {}".format(nic_name))
        except CloudError as e:
            if e.error and e.error.error == 'ResourceNotFound':
                logging.info("[app] Resource nic {} is not found".format(nic_name))
            else:
                raise e
        try:
            ip_addr = self.network_client.public_ip_addresses.get(RESOURCE_GROUP, ip_name)
            if ip_addr and ip_addr.tags and ip_addr.tags.get('node-uuid') == node_uuid:
                logging.info("[app] Deleting ip {}".format(ip_name))
                ip_del = self.network_client.public_ip_addresses.delete(RESOURCE_GROUP, ip_name)
                ip_del.wait()
                logging.info("[app] Deleted ip {}".format(ip_name))
        except CloudError as e:
            if e.error and e.error.error == 'ResourceNotFound':
                logging.info("[app] Resource ip name {} is not found".format(ip_name))
            else:
                raise e

        for disk_name, disk_del in disk_dels.items():
            disk_del.wait()
            logging.info("[app] Deleted disk {}".format(disk_name))

        logging.info("[app] Sucessfully destroyed orphaned resources for {}".format(vm_name))

    # The method is idempotent. Any failure raises exception such that it can be retried.
    def destroy_instance(self, vm_name, node_uuid):
        vm = self.compute_client.virtual_machines.get(RESOURCE_GROUP, vm_name)
        if not vm:
            logging.info("[app] VM {} is not found".format(vm_name))
            self.destroy_orphaned_resources(vm_name, node_uuid)
            return
        # Delete the VM first. Any subsequent failure will invoke the orphaned
        # resource deletion.
        logging.info("[app] Deleting vm {}".format(vm_name))
        vmdel = self.compute_client.virtual_machines.delete(RESOURCE_GROUP, vm_name)
        vmdel.wait()
        logging.info("[app] Deleted vm {}".format(vm_name))

        disk_dels = {}
        os_disk_name = vm.storage_profile.os_disk.name
        data_disks = vm.storage_profile.data_disks
        for disk in data_disks:
            logging.info("[app] Deleting disk {}".format(disk.name))
            disk_del = self.compute_client.disks.delete(RESOURCE_GROUP, disk.name)
            disk_dels[disk.name] = disk_del

        logging.info("[app] Deleting os disk {}".format(os_disk_name))
        disk_del = self.compute_client.disks.delete(RESOURCE_GROUP, os_disk_name)
        disk_dels[os_disk_name] = disk_del

        nic_name = self.get_nic_name(vm_name)
        ip_name = self.get_public_ip_name(vm_name)
        logging.info("[app] Deleting nic {}".format(nic_name))
        nic_del = self.network_client.network_interfaces.delete(RESOURCE_GROUP, nic_name)
        nic_del.wait()
        logging.info("[app] Deleted nic {}".format(nic_name))

        logging.info("[app] Deleting ip {}".format(ip_name))
        ip_del = self.network_client.public_ip_addresses.delete(RESOURCE_GROUP, ip_name)
        ip_del.wait()
        logging.info("[app] Deleted ip {}".format(ip_name))

        for disk_name, disk_del in disk_dels.items():
            disk_del.wait()
            logging.info("[app] Deleted disk {}".format(disk_name))

        logging.info("[app] Sucessfully destroyed instance {}".format(vm_name))

    def get_subnet_id(self, vnet, subnet):
        return SUBNET_ID_FORMAT_STRING.format(
            SUBSCRIPTION_ID, RESOURCE_GROUP, vnet, subnet
        )

    def get_nsg_id(self, nsg):
        if nsg:
            return NSG_ID_FORMAT_STRING.format(
                SUBSCRIPTION_ID, RESOURCE_GROUP, nsg
            )
        else:
            return

    def add_tag_resource(self, params, key, value):
        result = params.get("tags", {})
        result[key] = value
        params["tags"] = result
        params["storage_profile"]["osDisk"]["tags"] = result
        return params

    def create_or_update_vm(self, vm_name, zone, num_vols, private_key_file, volume_size,
                            instance_type, ssh_user, nsg, image, vol_type, server_type,
                            region, nic_id, tags, disk_iops, disk_throughput, is_edit=False,
                            json_output=True):
        disk_names = [vm_name + "-Disk-" + str(i) for i in range(1, num_vols + 1)]
        private_key = validated_key_file(private_key_file)

        shared_gallery_image_match = GALLERY_IMAGE_ID_REGEX.match(image)
        plan = None
        if shared_gallery_image_match:
            image_reference = {
                "id": image
            }
        else:
            # machine image URN - "OpenLogic:CentOS:7_8:7.8.2020051900"
            pub, offer, sku, version = image.split(':')
            image_reference = {
                "publisher": pub,
                "offer": offer,
                "sku": sku,
                "version": version
            }
            plan = self.compute_client.virtual_machine_images \
                .get(region, pub, offer, sku, version).as_dict().get('plan')

        # Base64 encode the cloud init data. Python base64 takes in and returns
        # byte-like objects, so we must encode the yaml and then decode the output.
        # This allows us to pass the cloud init as a base64 encoded string.
        cloud_init = cloud_init_encoded()
        vm_parameters = {
            "location": region,
            "os_profile": {
                "computer_name": vm_name,
                "admin_username": ssh_user,
                "linux_configuration": {
                    "disable_password_authentication": True,
                    "ssh": {
                        "public_keys": [{
                            "path": "/home/{}/.ssh/authorized_keys".format(ssh_user),
                            "key_data": format_rsa_key(private_key, public_key=True)
                        }]
                    }
                },
                "custom_data": cloud_init,
            },
            "hardware_profile": {
                "vm_size": instance_type
            },
            "storage_profile": {
                "osDisk": {
                    "createOption": "fromImage",
                    "managedDisk": {
                        "storageAccountType": "Standard_LRS"
                    }
                },
                "image_reference": image_reference
            },
            "network_profile": {
                "network_interfaces": [{
                    "id": nic_id
                }]
            }
        }
        if plan is not None:
            vm_parameters["plan"] = plan

        if zone is not None:
            vm_parameters["zones"] = [zone]

        if vol_type == ULTRASSD_LRS:
            vm_parameters["additionalCapabilities"] = {"ultraSSDEnabled": True}

        # Tag VM as cluster-server for ansible configure-{} script
        self.add_tag_resource(vm_parameters, "yb-server-type", server_type)
        for k in tags:
            self.add_tag_resource(vm_parameters, k, tags[k])
        creation_result = self.compute_client.virtual_machines.create_or_update(
            RESOURCE_GROUP,
            vm_name,
            vm_parameters
        )

        vm_result = creation_result.result()
        vm = self.compute_client.virtual_machines.get(RESOURCE_GROUP, vm_name)

        # Attach disks
        if is_edit:
            self.tag_disks(vm, vm_parameters["tags"])
        else:
            num_disks_attached = len(vm.storage_profile.data_disks)
            lun_indexes = []
            for idx, disk_name in enumerate(disk_names):
                # "Logical Unit Number" - where the data disk will be inserted. Add our disks
                # after any existing ones.
                lun = num_disks_attached + idx
                self.append_disk(
                    vm, vm_name, disk_name, volume_size, lun, zone, vol_type, region, tags,
                    disk_iops, disk_throughput)
                lun_indexes.append(lun)

            if json_output:
                return {"lun_indexes": lun_indexes}

    def query_vpc(self):
        """
        TODO: Similar to GCP, not implemented. Only used for getting default image.
        """
        return {}

    def format_zones(self, region, zones):
        return ["{}-{}".format(region, zone) for zone in zones]

    def get_zone_to_subnets(self, vnet, region):
        regionZones = self.metadata["regions"].get(region, {}).get("zones", [])
        zones = self.format_zones(region, regionZones)
        subnet = self.network().get_default_subnet(vnet)
        return {zone: subnet for zone in zones}

    def parse_vm_info(self, vm):
        vm_info = {}
        vm_info["numCores"] = vm.get("numberOfCores")
        vm_info["memSizeGb"] = float(vm.get("memoryInMB")) / 1000.0
        vm_info["maxDiskCount"] = vm.get("maxDataDiskCount")
        vm_info['prices'] = {}
        return vm_info

    def get_instance_types(self, regions):
        operation_start = datetime.datetime.now()

        # TODO: This regex probably should be refined? It returns a LOT of VMs right now.
        premium_regex_format = 'Standard_.*s'
        burstable_prefix = 'Standard_B'
        regex = re.compile(premium_regex_format, re.IGNORECASE)

        all_vms = {}
        # Base list of VMs to check for.
        vm_list = [vm.serialize() for vm in
                   self.compute_client.virtual_machine_sizes.list(location=regions[0])]
        for vm in vm_list:
            vm_size = vm.get("name")
            # We only care about VMs that support Premium storage. Promo is pricing special.
            if (vm_size.startswith(burstable_prefix) or not regex.match(vm_size)
                    or vm_size.endswith("Promo")):
                continue
            vm_info = self.parse_vm_info(vm)
            if vm_info["memSizeGb"] < MIN_MEM_SIZE_GB or vm_info["numCores"] < MIN_NUM_CORES:
                continue
            all_vms[vm_size] = vm_info

        workers = []
        for region in regions:
            worker = GetPriceWorker(region)
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()
            price_info = worker.vm_name_to_price_dict
            # Adding missed items.
            missed_price = set(all_vms.keys()) - set(price_info.keys())
            for vm_name in missed_price:
                price_info[vm_name] = {
                    "unit": "Hours",
                    "pricePerUnit": 0.0,
                    "pricePerHour": 0.0,
                    "pricePerDay": 0.0,
                    "pricePerMonth": 0.0,
                    "currency": "USD",
                    "effectiveDate": "2000-01-01T00:00:00.0000"
                }

            for vm_name in all_vms:
                all_vms[vm_name]['prices'][worker.region] = price_info[vm_name]

        execution_time = datetime.datetime.now() - operation_start
        logging.info("Finished price retrieving process [ %s ms ]",
                     execution_time.seconds * 1000 + execution_time.microseconds // 1000)
        return all_vms

    def ultra_ssd_available(self, capabilities):
        if not capabilities:
            return False
        for capability in capabilities:
            if (capability.get("name", None) == "UltraSSDAvailable"
                    and capability.get("value", None) == "True"):
                return True

    def get_ultra_instances(self, regions, folder):
        FOLDER_FORMAT = folder + "{}.json"
        tenant = os.environ['AZURE_TENANT_ID']
        authority_url = 'https://login.microsoftonline.com/' + tenant
        client_id = os.environ['AZURE_CLIENT_ID']
        client_secret = os.environ['AZURE_CLIENT_SECRET']
        resourceURL = 'https://management.azure.com/'
        context = adal.AuthenticationContext(authority_url)
        token = context.acquire_token_with_client_credentials(resourceURL, client_id, client_secret)
        headers = {'Authorization': 'Bearer ' + token['accessToken'],
                   'Content-Type': 'application/json'}
        for region in regions:
            vms = {}
            payload = {"api-version": "2019-04-01", "$filter": "location eq '{}'".format(region)}
            listOfResources = requests.get(RESOURCE_SKU_URL, params=payload,
                                           headers=headers).json().get("value", [])
            for resource in listOfResources:
                # We only care about virtual machines
                if resource.get("resourceType") != "virtualMachines":
                    continue
                # No special location info
                location_info = resource.get("locationInfo", [])
                if not location_info:
                    continue
                location_info = location_info[0]
                # No special zone info
                zone_details = location_info.get("zoneDetails", [])
                if not zone_details:
                    continue
                zone_details = zone_details[0]
                capabilities = zone_details.get("capabilities", [])
                # Checks zone details to see if detail is ultraSSD capability
                if self.ultra_ssd_available(capabilities):
                    instance_type = resource.get("name")
                    region = location_info.get("location")
                    zones = zone_details.get("Name")
                    yw_zones = self.format_zones(region, zones)
                    vms[instance_type] = yw_zones
            with open(FOLDER_FORMAT.format(region), 'w') as writefile:
                json.dump(vms, writefile)
        return

    def get_host_info(self, vm_name, get_all=False):
        try:
            vm = self.compute_client.virtual_machines.get(RESOURCE_GROUP, vm_name, 'instanceView')
        except Exception as e:
            return None
        nic_name = id_to_name(vm.network_profile.network_interfaces[0].id)
        nic = self.network_client.network_interfaces.get(RESOURCE_GROUP, nic_name)
        region = vm.location
        zone = vm.zones[0] if vm.zones else None
        private_ip = nic.ip_configurations[0].private_ip_address
        public_ip = None
        ip_name = None
        if (nic.ip_configurations[0].public_ip_address):
            ip_name = id_to_name(nic.ip_configurations[0].public_ip_address.id)
            public_ip = (self.network_client.public_ip_addresses
                         .get(RESOURCE_GROUP, ip_name).ip_address)
        subnet = id_to_name(nic.ip_configurations[0].subnet.id)
        server_type = vm.tags.get("yb-server-type", None) if vm.tags else None
        node_uuid = vm.tags.get("node-uuid", None) if vm.tags else None
        universe_uuid = vm.tags.get("universe-uuid", None) if vm.tags else None
        zone_full = "{}-{}".format(region, zone) if zone is not None else region
        instance_state = None
        if vm.instance_view is not None and vm.instance_view.statuses is not None:
            for status in vm.instance_view.statuses:
                logging.info("VM state {}".format(status.code))
                parts = status.code.split("/")
                if len(parts) != 2 or parts[0] != "PowerState":
                    continue
                instance_state = parts[1]
        is_running = True if instance_state == "running" else False
        return {"private_ip": private_ip, "public_ip": public_ip, "region": region,
                "zone": zone_full, "name": vm.name, "ip_name": ip_name,
                "instance_type": vm.hardware_profile.vm_size, "server_type": server_type,
                "subnet": subnet, "nic": nic_name, "id": vm.name, "node_uuid": node_uuid,
                "universe_uuid": universe_uuid, "instance_state": instance_state,
                "is_running": is_running}

    def get_dns_client(self, subscription_id):
        if self.dns_client is None:
            self.dns_client = PrivateDnsManagementClient(self.credentials,
                                                         subscription_id)
        return self.dns_client

    def list_dns_record_set(self, dns_zone_id):
        # Passing None as domain_name_prefix is not dangerous here as we are using subscription ID
        # only.
        _, subscr_id = self._get_dns_record_set_args(dns_zone_id, None)
        return self.get_dns_client(
            subscr_id).private_zones.get(*self._get_dns_zone_info(dns_zone_id))

    def create_dns_record_set(self, dns_zone_id, domain_name_prefix, ip_list):
        parameters, subscr_id = self._get_dns_record_set_args(
            dns_zone_id, domain_name_prefix, ip_list)
        # Setting if_none_match="*" will cause this to error if a record with the name exists.
        return self.get_dns_client(subscr_id).record_sets.create_or_update(if_none_match="*",
                                                                           **parameters)

    def edit_dns_record_set(self, dns_zone_id, domain_name_prefix, ip_list):
        parameters, subscr_id = self._get_dns_record_set_args(
            dns_zone_id, domain_name_prefix, ip_list)
        return self.get_dns_client(subscr_id).record_sets.update(**parameters)

    def delete_dns_record_set(self, dns_zone_id, domain_name_prefix):
        parameters, subscr_id = self._get_dns_record_set_args(dns_zone_id, domain_name_prefix)
        return self.get_dns_client(subscr_id).record_sets.delete(**parameters)

    def _get_dns_record_set_args(self, dns_zone_id, domain_name_prefix, ip_list=None):
        zone_info = PRIVATE_DNS_ZONE_ID_REGEX.match(dns_zone_id)
        rg, zone_name, subscr_id = self._get_dns_zone_info_long(dns_zone_id)
        args = {
            "resource_group_name": rg,
            "private_zone_name": zone_name,
            "record_type": "A",
            "relative_record_set_name": "{}.{}".format(domain_name_prefix, zone_name),
        }

        if ip_list is not None:
            params = {
                "ttl": DNS_RECORD_SET_TTL,
                "arecords": [{"ipv4_address": ip} for ip in ip_list]
            }
            args["parameters"] = params

        return args, subscr_id

    def _get_dns_zone_info_long(self, dns_zone_id):
        """Returns tuple of (resource_group, dns_zone_name, subscription_id).
        Assumes dns_zone_id is the zone name if it's not given in Resource ID format.
        """
        zone_info = PRIVATE_DNS_ZONE_ID_REGEX.match(dns_zone_id)
        if zone_info:
            return zone_info.group('resource_group'), zone_info.group(
                'zone_name'), zone_info.group('subscription_id')
        else:
            return RESOURCE_GROUP, dns_zone_id, SUBSCRIPTION_ID

    def _get_dns_zone_info(self, dns_zone_id):
        """Returns tuple of (resource_group, dns_zone_name). Assumes dns_zone_id is the zone name
        if it's not given in Resource ID format.
        """
        return self._get_dns_zone_info_long(dns_zone_id)[:2]

    def get_vm_status(self, vm_name):
        return (
            self.compute_client.virtual_machines.get(RESOURCE_GROUP,
                                                     vm_name,
                                                     expand='instanceView')
            .instance_view.statuses[1].display_status
        )

    def deallocate_instance(self, vm_name):
        async_vm_deallocate = self.compute_client.virtual_machines.deallocate(RESOURCE_GROUP,
                                                                              vm_name)
        async_vm_deallocate.wait()
        return async_vm_deallocate.result()

    def start_instance(self, vm_name):
        async_vm_start = self.compute_client.virtual_machines.start(RESOURCE_GROUP, vm_name)
        async_vm_start.wait()
        return async_vm_start.result()
