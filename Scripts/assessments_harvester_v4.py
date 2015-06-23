#import required modules
import exceptions
import urllib2, json, calendar, time
from cartodb import CartoDBAPIKey, CartoDBException

#cartodb variables
api_key = 'ebd93f0d0daf2ab7d2b31e2449e307cfe0744252'
cartodb_domain = 'troy'
cl = CartoDBAPIKey(api_key, cartodb_domain)
# define a variable to hold the source URL
urlData = "http://www.humanitarianresponse.info/api/v1.0/assessments"
#timestamp log for last run
epochtime = int(time.time())

#gets the last_timestamp from the log file
def get_timestamp_log():
    try:
        open_lastrun = open("lastrun.txt","r")
        if open_lastrun.mode == 'r':
            # check to make sure that the file was opened
            global last_timestamp
            last_timestamp = open_lastrun.read()
            print last_timestamp
    except:
        pass
get_timestamp_log()

#updates or creates the timestamp log and stores the last run timestamp
def update_timestamp_log():
    lastrun_log = open("lastrun.txt","w+")
    for i in range(1):
        lastrun_log.write(str(epochtime))
    lastrun_log.close()
    print str(epochtime)
    
      
def main():
  # Open the URL and read the data
  webUrl = urllib2.urlopen(urlData)
  if (webUrl.getcode() == 200):
      data = webUrl.read()
    
    # Use the json module to load the string data into a dictionary
      api_url = json.loads(data)
      
      list_ids = []
      
      for i in api_url["data"]:
        #defining the main level variables that will be pulled into CartoDB
        id = i["id"]
        label = i["label"]
        last_modified = i["changed"]
        date_created = i["created"]
        
        if int(last_modified) < last_timestamp:
            list_ids += [id]
        else:
            pass
        
        #get the clusters
        cluster_label = ""
        for b in i["bundles"]:
            if "label" in i["bundles"][0]:
                cluster_label += str(b["label"]).replace("'", "") + ", "
            else:
                cluster_label = "null"
        if cluster_label != "null":
            cluster_label = cluster_label[:-2]
        else:
            cluster_label = cluster_label
        
        #get the organizations
        org_label = ""
        for c in i["organizations"]:
            if "label" in i["organizations"][0]:
                org_label += str(c["label"]).replace("'", "") + ", "
            else:
                org_label = "null"
        if org_label != "null":
            org_label = org_label[:-2]
        else:
            org_label = org_label
        
        #get the themes
        theme_label = ""
        try:
            for t in i["themes"]:
                if "label" in i["themes"][0]:
                    theme_label += str(t["label"]).replace("'", "") + ", "
                else:
                    theme_label = "null"
            if theme_label != "null":
                theme_label = theme_label[:-2]
            else:
                theme_label = theme_label
        except TypeError:
            theme_label = "null"
        
        #get the themes
        disasters_label = ""
        try:
            for d in i["disasters"]:
                if "label" in i["disasters"][0]:
                    disasters_label += str(d["label"]).replace("'", "") + ", "
                else:
                    disasters_label = "null"
            if theme_label != "null":
                disasters_label = disasters_label[:-2]
            else:
                disasters_label = disasters_label
        except TypeError:
            disasters_label = "null"
            
        operation_label = str(i["operation"][0]["label"]).replace("'", "")
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
            
        other_location = i["other_location"]
        subject = i["subject"]
        methodology = i["methodology"]
        key_findings = i["key_findings"]
       
        date = i["date"]
        date_start = i["date"]["from"]
        date_end = i["date"]["to"]
        date_timezone = i["date"]["timezone"]
        
        frequency = i["frequency"]
        status = i["status"]
        
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
                    
        assessment_url = i["url"]
        
        for l in i["locations"]:  
            #variablea for nested locations JSON
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
                    geo_parent_url = load_locations["data"][0]["parent"][0]["self"]
                    open_geoparent = urllib2.urlopen(geo_parent_url)
                    if (open_geoparent.getcode() == 200):
                        geoparent_data = open_geoparent.read()
                        load_geoparent_data = json.loads(geoparent_data)
                        parent_geo_admin_level = load_geoparent_data["data"][0]["admin_level"]
                        if parent_geo_admin_level == "0":
                            country = load_geoparent_data["data"][0]["label"]
                        # finds, opens and loads the nested location url if necessary
                        else:
                            geo_grandparent_url = load_geoparent_data["data"][0]["parent"][0]["self"]
                            open_geograndparent = urllib2.urlopen(geo_grandparent_url)
                            if (open_geograndparent.getcode() == 200):
                                geo_grandparent_data = open_geograndparent.read()
                                load_geograndparent_data = json.loads(geo_grandparent_data)
                                grandparent_geo_admin_level = load_geograndparent_data["data"][0]["admin_level"]
                                if grandparent_geo_admin_level == "0":
                                    country = load_geograndparent_data["data"][0]["label"]
                                # finds, opens and loads the nested location url if necessary
                                else:
                                    geo_greatgrandparent_url = load_geograndparent_data["data"][0]["parent"][0]["self"]
                                    open_geogreatgrandparent = urllib2.urlopen(geo_greatgrandparent_url)
                                    if (open_geogreatgrandparent.getcode() == 200):
                                        geo_greatgrandparent_data = open_geogreatgrandparent.read()
                                        load_geogreatgrandparent_data = json.loads(geo_greatgrandparent_data)
                                        greatgrandparent_geo_admin_level = load_geogreatgrandparent_data["data"][0]["admin_level"]
                                        if greatgrandparent_geo_admin_level == "0":
                                            country = load_geogreatgrandparent_data["data"][0]["label"]
                                        else:
                                            #leaving as null for testing purposes if dive into 5th level
                                            country = "null"
                                    else:
                                        print "GreatGrandparent location url does not exist or cannot be opened. Code: " + str(open_geogreatgrandparent.getcode())
                            else:
                                print "Grandparent location url does not exist or cannot be opened. Code: " + str(open_geograndparent.getcode())
                            
                    else:
                        print "Parent location url does not exist or cannot be opened. Code: " + str(open_geoparent.getcode())
                        
            #prints error mesage if connection fails   
            else:
                print "Cannot open locations url. Code: " + str(openlocations.getcode())
      
            try:
                sql_query = "INSERT INTO humanitarian_response (the_geom, id, label, cluster_label, org_label, theme_label, disasters_label, operation_label, operation_status, operation_url, location_id, location_label, country, geoid, geo_pcode, geo_iso_code, geo_admin_level, other_location, subject, methodology, key_findings, date_start, date_end, date_timezone, frequency, status, report_id, report_filename, report_filesize, report_url, questionnaire_id, questionnaire_filename, questionnaire_filesize, questionnaire_url, data_upload_id, data_upload_filename, data_upload_filesize, data_upload_url, assessment_url) VALUES ("
                sql_query = sql_query + "'SRID=4326; POINT (%f %f)', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'" % (float(str(long)), float(str(lat)), id, label, cluster_label, org_label, theme_label, disasters_label, operation_label, operation_status, operation_url, location_id, location_label, country, geoid, geo_pcode, geo_iso_code, geo_admin_level, other_location, subject, methodology, key_findings, date_start, date_end, date_timezone, frequency, status, report_id, report_filename, report_filesize, report_url, questionnaire_id, questionnaire_filename, questionnaire_filesize, questionnaire_url, data_upload_id, data_upload_filename, data_upload_filesize, data_upload_url, assessment_url)
                sql_query = sql_query + ")"
                print str(sql_query)
            except ValueError,e:
                print ("some error ocurred", e)
            
            #try:
               # your CartoDB account:
                #print cl.sql(sql_query)
            #except CartoDBException as e:
               #print ("some error ocurred", e)
      
      print str(list_ids)
          
  else:
        print "Received an error from the server, cannot retrieve results " + str(webUrl.getcode())
        
if __name__ == "__main__":
  #update_timestamp_log()
  main()
  