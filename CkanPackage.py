from APIConnection import APIConnection
from APIException import APIException
from PackageException import PackageException
import logging.config

logging.config.fileConfig('config/logging.cfg')


class CkanPackage():
    def __init__(self, api_address):
        self.api = APIConnection(api_address)

    def load_package(self, id):
        try:
            self.package = self.api.get_package(id)
        except APIException as apiex:
            logging.error(apiex)
            raise PackageException("Problem to get package "+str(id))

    def exists_package(self, id):
        try:
            return self.api.check_package(id)
        except APIException as apiex:
            logging.error(apiex)
            return False

    def update(self,id, package):
        self.api.update_package(id,package)

    def get_package(self):
        return self.package

    def delete_metadata(self, list_metadata):
        for metadata in list_metadata:
            try:
                del self.package[metadata]
            except Exception as ex:
                logging.error()

    def get_organization_id(self):
        return self.package["organization"]["name"]

    def add_metadata(self, metadata, value):
        self.package[metadata] = value

    def get_metadata(self, metadata):
        return self.package[metadata]

    def write_package(self, package):
        self.api.write_package(package)



