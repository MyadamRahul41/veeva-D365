
############################################################# Minor version ##################################################
def get_spec_details_minorversion(spec_id):
    if not spec_id:
        return None
    query = f"SELECT specification_number__c, real_substance_specification_category_lu__c FROM formulation__v WHERE id = '{spec_id}'"
    result = run_query(query)
    return result[0] if result and result[0] else None

# --- Main Function ---

def minorversioncaller(current_time,deltadate,api_name):
    query = f"""
    SELECT id, description__c, specification_main_version__c,name__v,status__v,subtype__v,
    filename__v, document_creation_date__v,version_modified_date__v
    FROM documents 
    WHERE version_modified_date__v >= '{deltadate}' and version_modified_date__v < '{current_time}'    
    and specification_minor_version__c != null and (status__v = 'Approved' or status__v = 'Obsolete')
    """
    exsiting_versions = getexsitingversions()
    productlifecyclemapping = {
        'free_for_biology_state__c': 'R1 Biology',
        # '':'R2 Trials',
        'free_for_registration_state__c':'R3 Registration',
        'free_for_production_state__c': 'R4 Production',
        'obsolete_state__c': 'R5 Obsolete',
        'released_state__c':'Released',
        'active_state__c': 'R1 Biology'

        # '' : 'R6 Correction'
        }

    try:
        documents = run_query(query)
        documenttypemapping = {
            'Formulation Inert Specification' : 'ZSpec_IBDV_URL',
            'Composition of Formulation (CoF)' : 'ZSpec_IBDV_URL',
            'Specification Technical Material' : 'Zspec_TM_URL',
            'Formulation Specification' : 'Spec_URL',
            'Selection List' : 'ZSpec_SeL_URL'
            }
        ########
        statusmapping = {'Approved':'Approved',
                        'Released':'Approved',
                        'Obsolete':'Obsolete'}
        #######
        if not documents:
            print(f"WARNING : No documents retrieved or session ID is invalid. Exiting.")
            return
        else :
            print(f"Documents found for Minor versions during the timeframe: {documents}")
        for doc in documents:
            time.sleep(5) # Creating delay to free up used resources in d365 to avoid Inventable error
            doc_id = doc.get('id')
            subtype = doc.get('subtype__v')
            doc_desc = doc.get('name__v')
            status = doc.get('status__v')
            spec_id = doc.get('specification_main_version__c')
            spec_id = spec_id[0] if isinstance(spec_id, list) and spec_id else None
            spec_details = get_spec_details_minorversion(spec_id)

            spec_number = spec_details.get('specification_number__c') if spec_details else None
            spec_type = spec_details.get('real_substance_specification_category_lu__c') if spec_details else None
            documenttype = documenttypemapping.get(subtype)

            ###
            mappedstatus = statusmapping.get(status)
            ###
            item_num = spec_number.split('-')[0] if spec_number else None
            spec_details = get_spec_details_majorversion(spec_id)
            versionstate = spec_details.get('state__v') if spec_details else None
            productlifecycle = productlifecyclemapping.get(versionstate,'')
            file_content = download_file(doc_id)
            # document_url = ''
            if file_content:
                doc['item_num'] = item_num
                upload_status,upload_responce = upload_to_sharepoint(file_content,doc)
                print(f"status for upload_to_sharepoint:{upload_status}")
                update_metadata_to_sharepoint(upload_responce,doc,productlifecycle)
                # print(f"upload_responce:{upload_responce}")
                document_url = upload_responce['webUrl']
                print(f"documenturl:{document_url}")
            record = {
                "DataAreaId" : "Rd01",
                "Item Num": item_num,
                "FileName": doc_desc,
                "Document Type": documenttype,
                "Document Description": doc_desc,
                "Document URL": document_url,
                "Status" : status #Added to get the status of the document
                # "Status" : mappedstatus #Added to handle realeased

            }
            payload = {
                "_request" :
                {
            "AttachDocument" :[record]}}
            # api_name = 'Creation of Minor version through documents'
            api_name = 'minorversioncaller'
            if spec_number in exsiting_versions:
                try:
                    send_to_d365(payload,d365_minor_url, api_name)
                except D365RequestException as e:
                    failure_log(
                        e.status_code,
                        f"D365 Error in Sending Minor version document to D365 : {e.response_text}",
                        e.payload or payload,
                        d365_minor_url,
                        e.api_name,
                        "D365",
                        last_runtime=None
                    )
                except Exception as e:
                    failure_log(
                        0,
                        f"Unexpected error in D365 send: {str(e)}",
                        e.payload or payload,
                        d365_minor_url,
                        e.api_name,
                        "D365",
                        last_runtime=None
                )
            else:
                print("Corresponding Main version has not been created in D365, skipping the document sync")
    except Exception as e:
        custompayload = {'current_time':current_time,'deltadate':deltadate}
        failure_log(0, f'Error Occured in Minor version :{e}',custompayload,'','minorversioncaller',"Veeva", last_runtime=None)

