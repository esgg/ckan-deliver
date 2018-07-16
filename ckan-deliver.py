import threading

import requests
import json
import ConnectException
import os
from CkanPackage import CkanPackage
from CkanOrganization import CkanOrganization
import logging.config
from datetime import datetime
from PackageException import PackageException


logging.config.fileConfig('config/logging.cfg')

private_ckan_url = "http://inia.linkeddata.es/"
public_ckan_url = "http://public-inia.lab.oeg-upm.net/"

basic_action_url = "api/action/"

private_key = os.getenv('PRIVATE_API_URL','url')
timer = os.getenv('TIMER_IN_SEC','60')


def get_date():
    return "2017-11-09"

def get_last_revisions(choosen_date):
    url = private_ckan_url+basic_action_url+"revision_list"
    payload = {'since_time': choosen_date}
    request = requests.get(url, payload)
    return request.status_code, request.json()

def get_package_revision(revision_id):
    payload = {'id': revision_id}
    url = private_ckan_url+basic_action_url+"revision_show"
    request = requests.get(url, payload)
    if request.status_code != 200:
        raise ConnectException("Problem to show revisions")
    rest = request.json()
    return rest["result"]["packages"]

def get_packages(list_revisions):
    packages = []
    for revision in list_revisions["result"]:
        try:
            package_id = get_package_revision(revision)
            if package_id not in packages:
                packages.append(package_id)
        except ConnectException as cex:
            print cex.value
    return packages

def get_package(package_id):
    payload = {'id': package_id}
    url = private_ckan_url + basic_action_url + "package_show"
    request = requests.get(url, payload)
    if request.status_code != 200:
        raise ConnectException("Problem to show package")
    rest = request.json()
    return rest["result"]

def get_organization(name):
    payload = {'id': name}
    url = public_ckan_url+ basic_action_url + "organization_show"
    request = requests.get(url,payload)
    if request.status_code != 200:
        raise ConnectException("Problem to access organizazation "+name)
    rest = request.json()
    return rest

def get_tag_info(name):
    payload = {'id': name}
    url = public_ckan_url+ basic_action_url+ "tag_show"
    request = requests.get(url, data = payload)
    return request.status_code, request.json()

def create_tag(name):
    payload = {'name': name}
    headers = {'content-type': 'application/json','Authorization': private_key}
    url = public_ckan_url+ basic_action_url+"tag_create"
    request = requests.post(url, data=json.dumps(payload), headers=headers)
    if request.status_code != 200:
        raise ConnectException("Problem to create a tag "+name)
    rest = request.json()
    return rest

def create_package(payload):
    headers = {'content-type': 'application/json', 'Authorization': private_key}
    url = public_ckan_url + basic_action_url + "package_create"
    request = requests.post(url, data=json.dumps(payload), headers=headers)
    if request.status_code != 200:
        raise ConnectException("Problem to create a package")
    rest = request.json()
    return rest


time_mark = datetime.utcnow()
pidfile = "timestamp"
fileid = open(pidfile,"r+")

if os.path.exists(pidfile):
    time_mark = fileid.read()
    logging.info("Load timestamp:"+str(time_mark))

def task():
    global time_mark
    logging.info("Get last revisions from "+str(time_mark))
    status, revisions = get_last_revisions(time_mark)

    logging.info("Get list of packages ids")
    list_packages = get_packages(revisions)
    logging.debug(list_packages);


    #list_packages = ["test12"]
    list_not_used_metadata = ["id","metadata_created","creator_user_id","resources","metadata_modified","revision_id","owner_org","organization"]

    for package_id in list_packages:
        logging.info("Analyzing package "+str(package_id))
        try:
            #Load information from CKAN package
            package_private = CkanPackage("private_ckan")
            package_private.load_package(package_id)

            #Load information about organizarion from the public CKAN
            organization_private = CkanOrganization("public_ckan")
            organization_private.load_organization(package_private.get_organization_id())

            metadata_modified = package_private.get_metadata("metadata_modified")

            #Delete metadata related with the private CKAN
            package_private.delete_metadata(list_not_used_metadata)

            #Load information related with the organization in the public CKAN
            org_owner = organization_private.get_owner()

            #Add the organization metadata from the public CKAN to the package
            package_private.add_metadata("owner_org",org_owner)

            json_dump = package_private.get_package()
            #print json.dumps(json_dump, indent=4, sort_keys=True)
	    
            if json_dump["licencia_abierta"]=="si":	
               package_public = CkanPackage("public_ckan")
               if package_public.exists_package(package_id):
                  package_public.update(package_id,json_dump)
               else:
                  package_public.write_package(json_dump)
            
               logging.info("Package created/updated on public CKAN")
            else:
               logging.info("No open license") 

            fileid.seek(0)
            fileid.write(metadata_modified)

            time_mark = metadata_modified

            logging.info("Timestamp updated to "+metadata_modified)

        except PackageException as cpex:
            logging.error(str(cpex) + "on package "+str(package_id))

    logging.info("Waiting until next execution")
    threading.Timer(int(timer), task).start()

task()

#fileid.close()


