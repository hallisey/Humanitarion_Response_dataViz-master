#import required modules
import exceptions
import urllib2, json, calendar, time
from cartodb import CartoDBAPIKey, CartoDBException

#cartodb variables
api_key = 'cdce2379e95923bfdd7715fbf09a0ababdc1c3b5'
cartodb_domain = 'fis-ocha'
cl = CartoDBAPIKey(api_key, cartodb_domain)
# define a variable to hold the source URL
urlData = "http://www.humanitarianresponse.info/api/v1.0/assessments" #if there is a failure you can add ?page=# to the url and the script will pickup where it left off. 
#timestamp log for last run
epochtime = int(time.time())

#gets the last_timestamp from the log file
def get_timestamp_log():
    try:
        open_lastrun = open("lastrun.txt","r")
        if open_lastrun.mode == 'r':
            # check to make sure that the file was opened
            global last_timestamp
            last_timestamp = open_lastrun.read() #if you want to update all of the items then open the lastrun.txt document and change the value to 1.
    except:
        last_timestamp = 0
    print last_timestamp
    return last_timestamp

#updates or creates the timestamp log and stores the last run timestamp
def update_timestamp_log():
    lastrun_log = open("lastrun.txt","w+")
    for i in range(1):
        lastrun_log.write(str(epochtime))
    lastrun_log.close()
    print str(epochtime)

#executes the cartodb modifications    
def modify_cartodb(sql_query):
    try:
        # your CartoDB account:
        print cl.sql(sql_query)
    except CartoDBException as e:
        print ("some error ocurred", e)

# deletes rows from cartodb       
def delete_cartodb_sql(id):        
    try:
        sql_query = "DELETE FROM humanitarian_response WHERE id = "
        sql_query = sql_query + "'%s'" % str(id)
        print str(sql_query)
    except ValueError,e:
        print ("an error ocurred", e)
    modify_cartodb(sql_query)        

#inserts rows into cartodb                
def insert_cartodb_sql(lat, long, id, label, cluster_label, org_label, org_acronym, theme_label, disasters_label, operation_label, operation_status, operation_url, location_id, location_label, country, geoid, geo_pcode, geo_iso_code, geo_admin_level, other_location, subject, methodology, key_findings, date_start, date_end, date_start_text, date_end_text, date_timezone, frequency, status, report_id, report_filename, report_filesize, report_url, questionnaire_id, questionnaire_filename, questionnaire_filesize, questionnaire_url, data_upload_id, data_upload_filename, data_upload_filesize, data_upload_url, assessment_url, date_created, last_modified):
    try:
        sql_query = "INSERT INTO humanitarian_response (the_geom, id, label, cluster_label, org_label, org_acronym, theme_label, disasters_label, operation_label, operation_status, operation_url, location_id, location_label, country, geoid, geo_pcode, geo_iso_code, geo_admin_level, other_location, subject, methodology, key_findings, date_start, date_end, date_start_text, date_end_text, date_timezone, frequency, status, report_id, report_filename, report_filesize, report_url, questionnaire_id, questionnaire_filename, questionnaire_filesize, questionnaire_url, data_upload_id, data_upload_filename, data_upload_filesize, data_upload_url, assessment_url, date_created, last_modified) VALUES ("
        sql_query = sql_query + "'SRID=4326; POINT (%f %f)', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'" % (float(str(long)), float(str(lat)), id, label, cluster_label, org_label, org_acronym, theme_label, disasters_label, operation_label, operation_status, operation_url, location_id, location_label, country, geoid, geo_pcode, geo_iso_code, geo_admin_level, other_location, subject, methodology, key_findings, date_start, date_end, date_start_text, date_end_text, date_timezone, frequency, status, report_id, report_filename, report_filesize, report_url, questionnaire_id, questionnaire_filename, questionnaire_filesize, questionnaire_url, data_upload_id, data_upload_filename, data_upload_filesize, data_upload_url, assessment_url, date_created, last_modified)
        sql_query = sql_query + ")"
        print str(sql_query)
    except ValueError,e:
        print ("some error ocurred", e)
    modify_cartodb(sql_query)
    
def humanitarian_response_api(urlData):
  # Open the URL and read the data
  webUrl = urllib2.urlopen(urlData)
  if (webUrl.getcode() == 200):
      data = webUrl.read()
    
      # Use the json module to load the string data into a dictionary
      api_url = json.loads(data)
      
      # create placeholder list for parsed items
      insertable_items = []
            
      for i in api_url["data"]:
        #defining the main level variables that will be pulled into CartoDB
        id = i["id"]
        label = i["label"]
        last_modified = i["changed"]
        date_created = i["created"]
        
        #get the clusters
        try:
            cluster_label = ""
            try:
                for b in i["bundles"]:
                    if "label" in i["bundles"][0]:
                        cluster_api = b["self"]
                        openbundles = urllib2.urlopen(cluster_api)
                        if (openbundles.getcode() == 200):
                            bundles_data = openbundles.read()
                            load_bundles = json.loads(bundles_data)
                            if "global_cluster" in load_bundles["data"][0]:
                                bundles_label = load_bundles["data"][0]["global_cluster"]["label"]
                            else:
                                bundles_label = None
                        else:
                            print "Cannot open bundles url. Code: " + str(openbundles.getcode())    
                        if (bundles_label != None):
                            cluster_label += str(bundles_label).replace("'", "''") + ", "
                    else:
                        cluster_label = "null"
            except:
                cluster_label = "null"
            if cluster_label != "null":
                cluster_label = cluster_label[:-2]
            else:
                cluster_label = cluster_label
        except TypeError:
            cluster_label = "null"
        
        #get the organizations
        try:
            org_label = ""
            org_acronym = ""
            for c in i["organizations"]:
                if "label" in i["organizations"][0]:
                    
                    org_api = c["self"]
                    openorgs = urllib2.urlopen(org_api)
                    if (openorgs.getcode() == 200):
                        orgs_data = openorgs.read()
                        load_orgs = json.loads(orgs_data)
                        if "acronym" in load_orgs["data"][0]:
                            org_acronym += str(load_orgs["data"][0]["acronym"]) + ", "
                        else:
                            org_acronym = "null"
                    else:
                        print "Cannot open organizations url. Code: " + str(openorgs.getcode())
                        
                    org_label += str(c["label"]).replace("'", "''") + ", "
                else:
                    org_label = "null"
            if org_label != "null":
                org_label = org_label[:-2]
            else:
                org_label = org_label
            if org_acronym != "null":
                org_acronym = org_acronym[:-2]
            else:
                org_acronym = org_acronym
        except TypeError:
            org_label = "null"
            org_acronym = "null"
        
        #get the themes
        theme_label = ""
        try:
            for t in i["themes"]:
                if "label" in i["themes"][0]:
                    theme_label += str(t["label"]).replace("'", "''") + ", "
                else:
                    theme_label = "null"
            if theme_label != "null":
                theme_label = theme_label[:-2]
            else:
                theme_label = theme_label
        except TypeError:
            theme_label = "null"
        
        #get the disasters
        disasters_label = ""
        try:
            for d in i["disasters"]:
                if "label" in i["disasters"][0]:
                    disasters_label += str(d["label"]).replace("'", "''") + ", "
                else:
                    disasters_label = "null"
            if theme_label != "null":
                disasters_label = disasters_label[:-2]
            else:
                disasters_label = disasters_label
        except TypeError:
            disasters_label = "null"
            
        #get the operations
        try:
            operation_label = str(i["operation"][0]["label"]).replace("'", "''")
            operation_api = i["operation"][0]["self"]
            #open the nested operation url
            openoperation = urllib2.urlopen(operation_api)
            if (openoperation.getcode() == 200):
                operation_data = openoperation.read()
                load_operation = json.loads(operation_data)
                
                #defining the variables the are nested in the operation JSON
                operation_status = load_operation["data"][0]["status"]
                operation_url = load_operation["data"][0]["url"]
            else:
                print "Cannot open operation url. Code: " + str(openoperation.getcode())
        except KeyError:
            operation_label = "null"
            operation_status = "null"
            operation_url = "null"
            
        #other properties
        other_location = i["other_location"]
        subject = i["subject"]
        methodology = i["methodology"]
        key_findings = i["key_findings"]
       
        date = i["date"]
        date_start = i["date"]["from"]
        date_start_text = i["date"]["from"]
        date_end = i["date"]["to"]
        date_end_text = i["date"]["to"]
        date_timezone = i["date"]["timezone"]
        
        frequency = i["frequency"]
        status = i["status"]
        
        try:
            report = i["report"]
            report_access = i["report"]["accessibility"]
            if "file" in report:
                report_id = report["file"]["fid"]
                report_filename = report["file"]["filename"]
                report_filesize = report["file"]["filesize"]
                report_url = report["file"]["url"]
            else:
                report_id = "null"
                report_filename = "null"
                report_filesize = "null"
                report_url = "null"
        except KeyError:
                report_access = "null"
                report_id = "null"
                report_filename = "null"
                report_filesize = "null"
                report_url = "null"
        
        try:
            questionnaire = i["questionnaire"]
            questionnaire_access = i["questionnaire"]["accessibility"]
            if "file" in questionnaire:
                questionnaire_id = questionnaire["file"]["fid"]
                questionnaire_filename = questionnaire["file"]["filename"]
                questionnaire_filesize = questionnaire["file"]["filesize"]
                questionnaire_url = questionnaire["file"]["url"]
            else:
                questionnaire_id = "null"
                questionnaire_filename = "null"
                questionnaire_filesize = "null"
                questionnaire_url = "null"
        except KeyError:
                questionnaire_access = "null"
                questionnaire_id = "null"
                questionnaire_filename = "null"
                questionnaire_filesize = "null"
                questionnaire_url = "null"
        
        try:
            data_upload = i["data"]
            data_upload_access = i["data"]["accessibility"]
            if "file" in data_upload:
                data_upload_id = data_upload["file"]["fid"]
                data_upload_filename = data_upload["file"]["filename"]
                data_upload_filesize = data_upload["file"]["filesize"]
                data_upload_url = data_upload["file"]["url"]
            else:
                data_upload_id = "null"
                data_upload_filename = "null"
                data_upload_filesize = "null"
                data_upload_url = "null"
        except KeyError:
                data_upload_access = "null"
                data_upload_id = "null"
                data_upload_filename = "null"
                data_upload_filesize = "null"
                data_upload_url = "null"
                    
        assessment_url = i["url"]
        
        try:
            #get the properties pertaining to location
            for l in i["locations"]:  
                #variables for nested locations JSON
                location_api = l["self"]
                location_id = l["id"]
                location_label = l["label"]
                #checks connection and loads the json
                openlocations = urllib2.urlopen(location_api)
                if (openlocations.getcode() == 200):
                    location_data = openlocations.read()
                    load_locations = json.loads(location_data)
                    
                    #defining the variable to be inserted into cartodb from the nested JSON
                    geoid = load_locations["data"][0]["id"]
                    geo_pcode = load_locations["data"][0]["pcode"]
                    geo_iso_code = load_locations["data"][0]["iso3"]
                    geo_admin_level = load_locations["data"][0]["admin_level"]
                    lat = load_locations["data"][0]["geolocation"]["lat"]
                    long = load_locations["data"][0]["geolocation"]["lon"]
                    #dive into the nested locations to find admin level 0 which is the country name
                    country = ""
                    if geo_admin_level == "0":
                        country = location_label
                        #redeclares the location as null if the location is also the country
                        location_label = "null"
                    # finds, opens and loads the nested location url if necessary
                    else:
                        parent_exists = True
                        while parent_exists:
                            geo_parent_url = load_locations["data"][0]["parent"][0]["self"]
                            open_geoparent = urllib2.urlopen(geo_parent_url)
                            if (open_geoparent.getcode() == 200):
                                geoparent_data = open_geoparent.read()
                                load_locations = json.loads(geoparent_data)
                                parent_geo_admin_level = load_locations["data"][0]["admin_level"]
                                if parent_geo_admin_level == "0":
                                    country = load_locations["data"][0]["label"]
                                    parent_exists = False
                            else:
                                print "Parent location url does not exist or cannot be opened. Code: " + str(open_geoparent.getcode())
                            
                #prints error mesage if connection fails   
                else:
                    print "Cannot open locations url. Code: " + str(openlocations.getcode())
                
                #add parsed items to master list
                insertable_items += [(lat, long, id, label, cluster_label, org_label, org_acronym, theme_label, disasters_label, operation_label, operation_status, operation_url, location_id, location_label, country, geoid, geo_pcode, geo_iso_code, geo_admin_level, other_location, subject, methodology, key_findings, date_start, date_end, date_start_text, date_end_text, date_timezone, frequency, status, report_id, report_filename, report_filesize, report_url, questionnaire_id, questionnaire_filename, questionnaire_filesize, questionnaire_url, data_upload_id, data_upload_filename, data_upload_filesize, data_upload_url, assessment_url, date_created, last_modified)]
                print "Processing... ID: " + str(id) + " Clusters: " + str(cluster_label) +  " Location: " + str(location_label) + ", " + str(country) + " Organization: " + str(org_acronym)
        except:
            pass        
      print insertable_items
      
      #checks the timestamp to determine if items need to be updated or added
      for i in insertable_items:
          if (int(last_timestamp) == 0):
              pass #this is used on first run, you can insert all items by deleting "lastrun.txt" which will reset the timestamp to 0.
          elif int(i[44]) > int(last_timestamp): #if you add more fields, make sure that last_modified is always last and update the index number accordningly. You must also insert the empty column manually into the cartodb table in order to work
              delete_cartodb_sql(i[2])
      for i in insertable_items:
          if int(i[44]) > int(last_timestamp):       
              insert_cartodb_sql(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14], i[15], i[16], i[17], i[18], i[19], i[20], i[21], i[22], i[23], i[24], i[25], i[26], i[27], i[28], i[29], i[30], i[31], i[32], i[33], i[34], i[35], i[36], i[37], i[38], i[39], i[40], i[41], i[42], i[43], i[44])
      
      #loops through the API pages 
      if "next" in api_url:
          nextUrl = api_url["next"]["href"]
          print nextUrl
          return True, nextUrl
      
      return False, urlData
          
  else:
      print "Received an error from the server, cannot retrieve results " + str(webUrl.getcode())
  
 
 #main function   
if __name__ == "__main__":
  get_timestamp_log()
  next_exists = True
  while next_exists:
      next_exists, urlData = humanitarian_response_api(urlData)
  update_timestamp_log()  
