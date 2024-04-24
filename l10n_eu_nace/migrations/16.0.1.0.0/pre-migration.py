from odoo.upgrade import util
from collections import OrderedDict
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    # Move data from Model res.partner.nace to res.partner.industry
    # Rewrite res.partner relation with new res.partner.industry
    # First get all res.partner.nace
    # Then all relations from this model to res.partner m2o an m2m fields

    cr.execute("SELECT id, code, name, parent_id FROM res_partner_nace")
    parner_nace_ids = cr.fetchall()
    # get all unique nace_id so that update is mote efficient
    cr.execute("SELECT distinct nace_id FROM res_partner where nace_id is not null")
    nace_partner_rel = cr.fetchall()
    # m2m relation third form table
    cr.execute("SELECT res_partner_id, res_partner_nace_id FROM res_partner_res_partner_nace_rel")
    nace_partner_secondary_rel = cr.fetchall()

    # Order the records by the code column, being the letters first and then the numbers
    # TODO
    partner_nace_ids.sort(key = lambda item: ([str, int].index(type(item)), item))

    # map partner_nace_ids so that is a ordered dict with old id as key
    dict_partner_nace_ids = OrderedDict()
    for old_id, code, name, parent_id in parner_nace_ids:
        dict_partner_nace_ids[old_id] = {"code": code, "name": name, "parent_id": parent_id}

    # Create new res.partner.industry records
    for old_id, nace_data in dict_partner_nace_ids.items():
        code = nace_data.get("code", "")
        name = nace_data.get("name", "")
        old_parent_id = nace_data.get("parent_id", False)
        new_parent_id = dict_partner_nace_ids.get(old_parent_id, {}).get("new_id", False)
        query = f"""
            INSERT INTO res_partner_industry (name, full_name, parent_id)
            VALUES (
                {name},
                {code + " - " + name},
                {new_parent_id})
        """
        cr.execute("INSERT INTO res_partner_industry (code, name, parent_id) VALUES (%s, %s, %s)", (nace_data["code"], nace_data["name"], nace_data["parent_id"]))
        new_id = self.cr.fetchone()[0]
        # this will be used to update the relation in the res.partner model
        dict_partner_nace_ids[old_id].update({"new_id": new_id})

    # Update res.partner relation with new res.partner.industry
    # First the m2o relation
    for old_nace_id in nace_partner_rel:
        new_id = dict_partner_nace_ids[old_nace_id].get("new_id", False)
        cr.execute("UPDATE res_partner SET industry_id = %s WHERE nace_id = %s", (new_id, old_nace_id))

    # now the m2m relation
    for partner_id, old_id from nace_partner_secondary_rel:
        new_id = dict_partner_nace_ids[old_id].get("new_id", False)
        cr.execute("INSERT INTO res_partner_res_partner_industry_rel (res_partner_id, res_partner_industry_id) VALUES (%s, %s)", (partner_id, new_id))

    """I think that delete the old table is not necessary as will be wiped out by
    the cleanup. But I'm not sure to migrate the translations..."""
