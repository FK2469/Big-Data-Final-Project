# coding=utf-8

import json
import time

from pyspark import SparkContext

import utils

cluster2 = ['s3k6-pzi2.website.txt.gz', 'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz',
            'crc3-tcnm.CORE_SUBJECT.txt.gz',
            'aiww-p3af.Location.txt.gz', 'jhjm-vsp8.Agency.txt.gz',
            '3rfa-3xsf.Incident_Zip.txt.gz',
            'pqg4-dm6b.Phone.txt.gz', 'mdcw-n682.First_Name.txt.gz',
            'a5td-mswe.Vehicle_Color.txt.gz',
            '2bnn-yakx.Vehicle_Color.txt.gz', 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz',
            'jz4z-kudi.Violation_Location__City_.txt.gz',
            'ge8j-uqbf.interest.txt.gz',
            'niy5-4j7q.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Neighborhood.txt.gz',
            '446w-773i.Address_1.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz',
            'mdcw-n682.Last_Name.txt.gz',
            '52dp-yji6.Owner_Last_Name.txt.gz', '43nn-pn8j.DBA.txt.gz',
            '5fn4-dr26.City.txt.gz',
            '7yay-m4ae.AGENCY_NAME.txt.gz',
            'wcmg-48ep.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
            'vw9i-7mzq.interest1.txt.gz',
            '3miu-myq2.COMPARABLE_RENTAL___2__Neighborhood.txt.gz',
            'upwt-zvh3.SCHOOL_LEVEL_.txt.gz',
            'uqxv-h2se.independentwebsite.txt.gz',
            'yjub-udmw.Location__Lat__Long_.txt.gz', 'f3cg-u8bv.Agency.txt.gz',
            'ahjc-fdu3.PRINCIPAL_PHONE_NUMBER.txt.gz',
            'a2pm-dj2w.Borough.txt.gz', 'vhah-kvpj.Borough.txt.gz',
            'yg5a-hytu.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
            'qcdj-rwhu.BUSINESS_NAME2.txt.gz',
            'n3p6-zve2.website.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz',
            'pdpg-nn8i.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz',
            '2bnn-yakx.Vehicle_Body_Type.txt.gz',
            'ji82-xba5.address.txt.gz', 'kiv2-tbus.Vehicle_Color.txt.gz',
            'q5za-zqz7.Agency.txt.gz',
            'fb26-34vu.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz',
            'yahh-6yjc.School_Type.txt.gz',
            'kiv2-tbus.Vehicle_Make.txt.gz',
            'faiq-9dfq.Vehicle_Body_Type.txt.gz',
            'niy5-4j7q.COMPARABLE_RENTAL___2__Building_Classification.txt.gz',
            's3k6-pzi2.interest1.txt.gz',
            'm59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
            'fzv4-jan3.SCHOOL_LEVEL_.txt.gz',
            's9d3-x4fz.ZIP.txt.gz', 'jt7v-77mi.Vehicle_Color.txt.gz',
            'cspg-yi7g.PHONE.txt.gz', 'vx8i-nprf.MI.txt.gz',
            'qgea-i56i.PREM_TYP_DESC.txt.gz', 'mjux-q9d4.SCHOOL.txt.gz',
            'cspg-yi7g.ADDRESS.txt.gz',
            'qusa-igsv.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
            'ipu4-2q9a.Owner_s_House_Zip_Code.txt.gz',
            'va74-3m6c.company_phone.txt.gz', 'a9md-ynri.MI.txt.gz',
            'n84m-kx4j.VEHICLE_MAKE.txt.gz',
            '8gpu-s594.SCHOOL_NAME.txt.gz', 'tg3t-nh4h.BusinessName.txt.gz',
            'mu46-p9is.Location.txt.gz',
            '4s7y-vm5x.CORE_SUBJECT.txt.gz', '3rfa-3xsf.Street_Name.txt.gz',
            'niy5-4j7q.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
            '6anw-twe4.FirstName.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz',
            'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz',
            '2bnn-yakx.Vehicle_Make.txt.gz',
            'hy4q-igkk.Intersection_Street_2.txt.gz',
            'sxmw-f24h.Street_Name.txt.gz',
            'sqcr-6mww.Park_Facility_Name.txt.gz', '6rrm-vxj9.parkname.txt.gz',
            'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz',
            'cvh6-nmyi.SCHOOL.txt.gz',
            '4kpn-sezh.Website.txt.gz',
            't8hj-ruu2.License_Business_City.txt.gz',
            '2sps-j9st.PERSON_LAST_NAME.txt.gz',
            'vwxi-2r5k.CORE_SUBJECT.txt.gz', 'myei-c3fa.Neighborhood_2.txt.gz',
            '2bmr-jdsv.DBA.txt.gz',
            '5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
            'uq7m-95z8.neighborhood.txt.gz',
            'pvqr-7yc4.Vehicle_Make.txt.gz',
            'nhms-9u6g.Name__Last__First_.txt.gz', 'f4rp-2kvy.Street.txt.gz',
            '3btx-p4av.COMPARABLE_RENTAL___1__Neighborhood.txt.gz',
            'qpm9-j523.org_neighborhood.txt.gz',
            'bbs3-q5us.ZIP.txt.gz', 'erm2-nwe9.Landmark.txt.gz',
            '72ss-25qh.Borough.txt.gz',
            'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz',
            's27g-2w3u.School_Name.txt.gz',
            'yayv-apxh.SCHOOL_LEVEL_.txt.gz',
            'emnd-d8ba.PRINCIPAL_PHONE_NUMBER.txt.gz',
            'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
            's79c-jgrm.City.txt.gz',
            'uzcy-9puk.Intersection_Street_2.txt.gz',
            '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz',
            '6bgk-3dad.RESPONDENT_CITY.txt.gz', 'kwmq-dbub.CANDMI.txt.gz',
            'uzcy-9puk.Street_Name.txt.gz',
            'uchs-jqh4.School_Name.txt.gz',
            'yrf7-4wry.COMPARABLE_RENTAL___2___Neighborhood.txt.gz',
            'brga-xeqy.Agency.txt.gz', 'bs8b-p36w.LOCATION.txt.gz',
            'i9pf-sj7c.INTEREST.txt.gz',
            't8hj-ruu2.First_Name.txt.gz',
            'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz',
            '5uac-w243.PREM_TYP_DESC.txt.gz',
            'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz',
            '6je4-4x7e.SCHOOL_LEVEL_.txt.gz', '4pt5-3vv4.Location.txt.gz',
            '4d7f-74pe.Agency.txt.gz', '735p-zed8.CANDMI.txt.gz',
            'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz',
            'e9xc-u3ds.ZIP.txt.gz', '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz',
            '3rfa-3xsf.Cross_Street_2.txt.gz',
            'vrn4-2abs.SCHOOL_LEVEL_.txt.gz',
            '4hiy-398i.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
            'ajxm-kzmj.NeighborhoodName.txt.gz',
            'ydkf-mpxb.CrossStreetName.txt.gz',
            'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz',
            '9z9b-6hvk.Borough.txt.gz', 'sxmw-f24h.School_Name.txt.gz',
            'kwmq-dbub.CITY.txt.gz', 'faiq-9dfq.Vehicle_Color.txt.gz',
            'fbaw-uq4e.Location_1.txt.gz',
            'erm2-nwe9.Incident_Zip.txt.gz',
            '8eux-rfe8.INPUT_1_Borough.txt.gz',
            't8hj-ruu2.Business_Phone_Number.txt.gz',
            'sxmw-f24h.Cross_Street_1.txt.gz',
            'w9ak-ipjd.Owner_s_Business_Name.txt.gz',
            'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz',
            'aiww-p3af.Street_Name.txt.gz',
            'szkz-syh6.Prequalified_Vendor_Address.txt.gz',
            'jt7v-77mi.Vehicle_Make.txt.gz', 'pq5i-thsu.DVC_MAKE.txt.gz',
            'mqdy-gu73.Color.txt.gz',
            '3rfa-3xsf.Park_Facility_Name.txt.gz',
            '52dp-yji6.Owner_First_Name.txt.gz',
            'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz', '9b9u-8989.DBA.txt.gz',
            'as69-ew8f.StartCity.txt.gz',
            'qu8g-sxqf.MI.txt.gz',
            '956m-xy24.COMPARABLE_RENTAL_2__Building_Classification.txt.gz',
            'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz',
            'kiv2-tbus.Vehicle_Body_Type.txt.gz',
            'pvqr-7yc4.Vehicle_Make.txt.gz',
            's3k6-pzi2.interest4.txt.gz', 'bdjm-n7q4.CrossStreet2.txt.gz',
            'sqcr-6mww.Cross_Street_2.txt.gz',
            'cgz5-877h.SCHOOL_LEVEL_.txt.gz',
            'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
            'vw9i-7mzq.interest3.txt.gz', 'ic3t-wcy2.Zip.txt.gz',
            'k3cd-yu9d.CANDMI.txt.gz',
            'bjuu-44hx.DVV_MAKE.txt.gz',
            'w6yt-hctp.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
            'ic3t-wcy2.Applicant_s_First_Name.txt.gz',
            'pvqr-7yc4.Vehicle_Color.txt.gz', 'crc3-tcnm.BOROUGH.txt.gz',
            'sqcr-6mww.Location.txt.gz',
            '9b9u-8989.Establishment_Street.txt.gz',
            'kj4p-ruqc.StreetName.txt.gz',
            'pvqr-7yc4.Vehicle_Body_Type.txt.gz',
            'jz4z-kudi.Respondent_Address__City_.txt.gz',
            'hy4q-igkk.Cross_Street_1.txt.gz', '922w-z7da.address_1.txt.gz',
            'w9ak-ipjd.Filing_Representative_First_Name.txt.gz',
            '9ck8-hj3u.PRINCIPAL_PHONE_NUMBER.txt.gz',
            '4twk-9yq2.CrossStreet2.txt.gz', 'mq9d-au8i.School_Name.txt.gz',
            'r4c5-ndkx._Phone.txt.gz',
            'rtws-c2ai.School_Name.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz',
            '4twk-9yq2.CrossStreet2.txt.gz',
            'w9if-3pyn.School_Name.txt.gz', 'uq7m-95z8.interest1.txt.gz',
            'n2s5-fumm.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
            '72ss-25qh.Agency_ID.txt.gz',
            'pvqr-7yc4.Vehicle_Body_Type.txt.gz', 'bdjm-n7q4.Location.txt.gz',
            'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz',
            'ebkm-iyma.Street_Address.txt.gz',
            'tqtj-sjs8.FromStreetName.txt.gz', 'uzcy-9puk.Incident_Zip.txt.gz',
            'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
            'e9xc-u3ds.CANDMI.txt.gz',
            '7btz-mnc8.Provider_First_Name.txt.gz',
            '8586-3zfm.School_Name.txt.gz', 'aiww-p3af.School_Name.txt.gz',
            'vr8p-8shw.DVT_MAKE.txt.gz', 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz',
            'ci93-uc8s.Website.txt.gz',
            '7btz-mnc8.Provider_Last_Name.txt.gz',
            'vw9i-7mzq.interest4.txt.gz', 'as69-ew8f.TruckMake.txt.gz',
            'xck4-5xd5.website.txt.gz', 'dpm2-m9mq.owner_city.txt.gz',
            'p937-wjvj.HOUSE_NUMBER.txt.gz',
            'f42p-xqaa.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
            'fxdy-q85h.Agency.txt.gz', 'w9ak-ipjd.City.txt.gz',
            'ci93-uc8s.Vendor_DBA.txt.gz',
            'ccgt-mp8e.Borough.txt.gz', '5cd6-v74i.School_Name.txt.gz',
            'uq7m-95z8.interest6.txt.gz',
            'mdcw-n682.Agency_Website.txt.gz',
            'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz',
            'aiww-p3af.Incident_Zip.txt.gz',
            '2v9c-2k7f.DBA.txt.gz', 'dtmw-avzj.BOROUGH.txt.gz',
            'ci93-uc8s.telephone.txt.gz',
            'uwyv-629c.StreetName.txt.gz',
            '3btx-p4av.COMPARABLE_RENTAL___1__Building_Classification.txt.gz',
            'i8ys-e4pm.CORE_SUBJECT_9_12_ONLY_.txt.gz',
            'i6b5-j7bu.TOSTREETNAME.txt.gz', 'gez6-674h.BORO.txt.gz',
            'mdcw-n682.Middle_Initial.txt.gz', 'pdk7-puui.Agency.txt.gz',
            'p937-wjvj.LOCATION.txt.gz',
            'ph7v-u5f3.WEBSITE.txt.gz', 'iz2q-9x8d.Owner_s__Phone__.txt.gz',
            '4qii-5cz9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
            'ruce-cnp6.Agency.txt.gz',
            'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz', '8gqz-6v9v.Website.txt.gz',
            'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz',
            'hy4q-igkk.Park_Facility_Name.txt.gz',
            'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz',
            'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz',
            'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz',
            'ptev-4hud.City.txt.gz', 'sv2w-rv3k.BORO.txt.gz',
            'n2mv-q2ia.Address_1__self_reported_.txt.gz',
            'faiq-9dfq.Vehicle_Body_Type.txt.gz',
            'myei-c3fa.Neighborhood.txt.gz',
            'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz',
            'uzcy-9puk.School_Name.txt.gz',
            '8u86-bviy.Address_1__self_reported_.txt.gz',
            'y4fw-iqfr.Address.txt.gz', 'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz',
            'sxx4-xhzg.Park_Site_Name.txt.gz', '4e2n-s75z.Address.txt.gz',
            'dm9a-ab7w.STREET_NAME.txt.gz',
            's3k6-pzi2.interest5.txt.gz', 'c284-tqph.Vehicle_Color.txt.gz',
            'wks3-66bn.School_Name.txt.gz',
            '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
            'c284-tqph.Vehicle_Make.txt.gz',
            '3btx-p4av.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
            'urc8-8bjy.BORO.txt.gz', '6anw-twe4.LastName.txt.gz',
            'yvxd-uipr.Location_1.txt.gz',
            'vwxi-2r5k.BOROUGH.txt.gz', '7jkp-5w5g.Agency.txt.gz',
            's3zn-tf7c.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
            'qu8g-sxqf.First_Name.txt.gz',
            'erm2-nwe9.Park_Facility_Name.txt.gz',
            '2bnn-yakx.Vehicle_Make.txt.gz', 'cvse-perd.Agency_Name.txt.gz',
            'rmv8-86p4.BROOKLYN_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
            'xubg-57si.OWNER_BUS_STREET_NAME.txt.gz',
            '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz',
            '735p-zed8.CITY.txt.gz', 'w9ak-ipjd.Applicant_Last_Name.txt.gz',
            'pqg4-dm6b.Address1.txt.gz',
            '2bnn-yakx.Vehicle_Body_Type.txt.gz',
            'urzf-q2g5.Phone_Number.txt.gz']


class Semantic:
    def __init__(self, semantic_type="", count=0, _label=""):
        self.semantic_type = semantic_type
        self.count = count

        if semantic_type == "other":
            self.label = _label


sc = SparkContext()


class NameDataset:
    FIRST_NAME_SEARCH = 'FIRST_NAME_SEARCH'
    LAST_NAME_SEARCH = 'LAST_NAME_SEARCH'

    def __init__(self, first_names_file, last_names_file):
        self.first_names = set(first_names_file.map(lambda x: x.strip()).collect())
        self.last_names = set(last_names_file.map(lambda x: x.strip()).collect())

    def _search_name(self, name, name_type):
        names = self.first_names if name_type == NameDataset.FIRST_NAME_SEARCH else self.last_names
        return name.strip().lower() in names

    def search_first_name(self, first_name):
        try:
            first_name, last_name = first_name.split(',')
            return self._search_name(first_name, name_type=NameDataset.FIRST_NAME_SEARCH)
        except:
            pass

        return self._search_name(first_name, name_type=NameDataset.FIRST_NAME_SEARCH)

    def search_last_name(self, last_name):
        try:
            first_name, last_name = last_name.split(',')
            return self._search_name(first_name, name_type=NameDataset.FIRST_NAME_SEARCH)
        except:
            pass

        return self._search_name(last_name, name_type=NameDataset.LAST_NAME_SEARCH)


m = NameDataset(sc.textFile("first_names.all.txt"), sc.textFile('last_names.all.txt'))


def valid_person_name(value):
    return m.search_first_name(value) or m.search_last_name(value)


utils.semantic_type_validator["person_name"] = valid_person_name

task2_json = []

start = time.time()
for item in cluster2:
    print("Handling: ", item)
    semantic_types = []
    curr_file = sc.textFile('/user/hm74/NYCColumns/' + item, 1).map(lambda x: x.split("\t")).filter(
        lambda x: str(x[0]) != 'UNSPECIFIED')
    total = curr_file.map(lambda x: int(x[1])).sum()
    for semantic_type_, validator in utils.semantic_type_validator.items():
        sub_sum = curr_file.filter(lambda x: validator(str(x[0])) is True).map(lambda x: int(x[1])).sum()
        if sub_sum / total >= 0.4:
            semantic_types.append(Semantic(semantic_type_, sub_sum).__dict__)

    if len(semantic_types) == 0:
        for label, validator in utils.semantic_type_checker.items():
            if validator(utils.process_column_name(item)):
                semantic_types = [Semantic(label, total).__dict__]
                break
        else:
            semantic_types = [Semantic("other", total, _label=utils.column_name_to_label(item)).__dict__]

    task2_json.append({
        "column_name": item[:-7],
        "semantic_types": semantic_types,
    })

print("Used time:", str(time.time() - start))

with open("./output/" + "task2.json", "w") as fp:
    json.dump(task2_json, fp)
