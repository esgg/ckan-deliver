from APIConnection import APIConnection
from APIException import APIException
import OrganizationException
import logging.config

logging.config.fileConfig('config/logging.cfg')


class CkanOrganization():
    def __init__(self, ckan):
        self.api = APIConnection(ckan)

    def load_organization(self, id):
        try:
            self.organization = self.api.get_organization(id)
        except APIException as apiex:
            logging.error(apiex)
            raise OrganizationException("Problem to get organizarion " + str(id))

    def get_owner(self):
        return self.organization["id"]

