#import necessary modules
import urllib, json
from cartodb import CartoDBAPIKey, CartoDBException

#define source data URL, open it, and read it
url = "http://www.humanitarianresponse.info/api/v1.0/assessments"
response = urllib.urlopen(url);
data = json.loads(response.read())

api_key = 'ebd93f0d0daf2ab7d2b31e2449e307cfe0744252'
cartodb_domain = 'troy'
cl = CartoDBAPIKey(api_key, cartodb_domain)

#print out 
print data

##next step: parse through response and feed to CartoDB table

#main function
if __name__ == '__main__':