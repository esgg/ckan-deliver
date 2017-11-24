import requests
import ConnectException
import json
from APIException import APIException

private_ckan_user = ""
private_ckan_key = ""

public_ckan_user = ""
public_ckan_key = ""



private_ckan_url = "http://inia.linkeddata.es/"
public_ckan_url = "http://public-inia.lab.oeg-upm.net/"

class APIConnection:

    def __init__(self, id):
        if id =="private_ckan":
            self.base_url = private_ckan_url
            self.ckan_user = private_ckan_user
            self.ckan_private_key = private_ckan_key
        elif id == "public_ckan":
            self.base_url = public_ckan_url
            self.ckan_user = public_ckan_user
            self.ckan_private_key = public_ckan_key

        self.base_url = self.base_url + "api/action/"

    def get_package(self, package_id):
        url = self.base_url+"package_show"
        payload = {'id': package_id}
        request = requests.get(url, params=payload, headers={'Authorization': self.ckan_private_key})
        if request.status_code != 200:
            raise APIException("Problem detected "+ str(request.status_code))
        rest = request.json()
        return rest["result"]

    def check_package(self,id):
        url = self.base_url + "package_show"
        payload = {'id': id}
        request = requests.get(url, params=payload, headers={'Authorization': self.ckan_private_key})
        if request.status_code != 200:
            raise APIException("Problem detected " + str(request.status_code))
        rest = request.json()
        return rest["success"]

    def update_package(self,id,package):
        url = self.base_url + "package_update"
        request = requests.post(url, data=json.dumps(package),
                                headers={'content-type': 'application/json', 'Authorization': self.ckan_private_key})
        if request.status_code != 200:
            raise APIException("Problem detected " + str(request.status_code))

    def get_organization(self, organization_id):
        url = self.base_url + "organization_show"
        payload = {'id': organization_id}
        request = requests.get(url, params=payload, headers={'Authorization': self.ckan_private_key})
        if request.status_code != 200:
            raise APIException("Problem detected " + str(request.status_code))
        rest = request.json()
        return rest["result"]

    def write_package(self, package):
        url = self.base_url + "package_create"
        request = requests.post(url, data = json.dumps(package), headers = {'content-type': 'application/json', 'Authorization': self.ckan_private_key})
        if request.status_code != 200:
            raise APIException("Problem detected "+ str(request.status_code))
