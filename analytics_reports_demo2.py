#Transo - app Analytics and Reports

import requests
import json
import flask
import datetime
from time import sleep
from datetime import time,timedelta
from flask import Flask, jsonify, request,json,make_response
from flask_cors import CORS
import pandas as pd
import numpy as np
import psycopg2
from itertools import chain, groupby
import math
import gzip

app = flask.Flask(__name__)
CORS(app)
app.config["DEBUG"] = True

#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------


# --------------------------------------------------- OPERATIONAL REPORTS -------------------------------------------------

#TRIP DETAILS REPORT

@app.route('/tripDetailsReport', methods = ['GET'])

def tripDetailsReport():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	limit = request.args.get('perPage')
	offset = request.args.get('page')
	fromDate = request.args.get('fromDate')
	toDate = request.args.get('toDate')
	vendorName = request.args.get('vendorName')
	vehicleNumber = request.args.get('vehicleNumber')

	query_string = ""
	if fromDate and toDate and vendorName and vehicleNumber:
		query_string = "where trip_date between '{}' and '{}' and vendor_name = '{}' and vehicle_number = '{}'".format(fromDate,toDate,vendorName,vehicleNumber)
	
	elif fromDate and toDate:
		if vehicleNumber:
			query_string = "where trip_date between '{}' and '{}' and vehicle_number = '{}'".format(fromDate,toDate,vehicleNumber)
		elif vendorName:
			query_string = "where trip_date between '{}' and '{}' and vendor_name = '{}'".format(fromDate,toDate,vendorName)
		else:
			query_string = "where trip_date between '{}' and '{}'".format(fromDate,toDate)

	elif vendorName:
		query_string = "where vendor_name = '{}'".format(vendorName)

	elif vehicleNumber:
		query_string = "where vehicle_number = '{}'".format(vehicleNumber)

	elif vendorName and vehicleNumber:
		query_string = "where vendor_name = '{}' and vehicle_number = '{}'".format(vendorName,vehicleNumber)

	print("QUERY --------->",query_string)
		

	if companyId:
		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT DISTINCT SHIPMENT_DETAILS.LRNO, 
vehicle.regno as vehicle_number,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null 
	then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
	else company.company_name end as vendor_name,
	trip.time_stamp::date as trip_date
		FROM TRIP_TRACK
		LEFT JOIN TRIP ON TRIP.ID = TRIP_TRACK.TRIP_ID
		LEFT JOIN SHIPMENT_DETAILS ON SHIPMENT_DETAILS.DROP_ID = TRIP_TRACK.DROP_ID
		LEFT JOIN TRIP_CONSIGNMENT ON TRIP_TRACK.DROP_ID = TRIP_CONSIGNMENT.DROP_ID
		LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
		LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
		LEFT JOIN VEHICLE ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
		LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
		INNER JOIN USERS ON USERS.ID = BOOKING.USER_ID
		LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
		join drops on drops.id = trip_track.drop_id
		left join source on booking.source_id = source.id
		left join gps_device_provider on gps_device_provider.id = vehicle.gps_device_provider
		WHERE company.id = {}
						AND TRIP.TRIP_STATUS != 'Canceled') mytable {}""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["lr_number","vehicle_number","vendor_name","trip_date"])

		q = """select * from (select distinct trip.id,vehicle.id as vehicle_id,
	vehicle.regno as vehicle_number,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null 
	then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
	else company.company_name end as vendor_name,
	driver.firstname as driver_name,
	driver.phone_mobile as driver_number,
	trip.time_stamp::date as trip_date,
	to_char(trip.time_stamp::time, 'HH24:MM') as trip_time,
	trip.trip_status as trip_status,
	trip.shipment_id as shipment_id,
	SHIPMENT_DETAILS.LRNO AS LR_NO,
	concat(vehicle_status.lattitude,',',vehicle_status.longitude) as last_location,
	vehicle_status.location_update_time::date as last_location_date,
	source.name as consignor,
	drops.name as consignee,
	TRIP_EVENT_START.event_time::date as trip_dispatch_date,
	booking_commercial.logistic_booking_type as booking_type,
	trip.TRIP_STATUS AS CURRENT_TRIP_STATUS,
	TRIP_EVENT_END.event_time::date as trip_delivery_date,
	case 
			when t2.eta-t3.actual_delivery_date > (INTERVAL '0 hours 0 minutes') 
			then 'On Time' 
			when t2.eta-t3.actual_delivery_date < (INTERVAL '0 hours 0 minutes') then 'Arriving early' else 'On Time'
			end as trip_eta_status,
	trip.current_eta::date-trip.actual_eta::date as delay_duration,
	trip.current_eta::date as eta,
	round(TRIP.ACTUAL_DISTANCE) AS TRIP_DISTANCE,
	round(TRIP.trip_km) AS DISTANCE_COVERED,
	round(TRIP.REM_DISTANCE::decimal) AS REMAINING_DISTANCE,
	total_violations.violation_count as total_violations,
	suddenStoppage.suddenStoppage as sudden_stoppage,
	harshAcceleration.harshAcceleration as harsh_acceleration,
	harshBraking.harshBraking as harsh_braking,
	harshManeuver.harshManeuver as harsh_maneuver,
	overSpeeding.overSpeeding as overSpeeding,
	nighDriving.nighDriving as night_driving,
	continuousDriving.continuousDriving as continuous_driving,
	sos.sos as sos,
	distraction.distraction as distraction
		from
		trip_track
		join trip on trip.id = trip_track.trip_id
		join shipment_details on shipment_details.drop_id = trip_track.drop_id
		join drops on drops.id = trip_track.drop_id

		join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
		join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
		left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
		left join vehicle_status on vehicle_status.vehicle_id = vehicle.id
		join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
		join vehicle_type on vehicle_attr. vehicle_type_id = vehicle_type.id
		join booking on booking.id = vehicle_booking_details.booking_id
		left join source on booking.source_id = source.id
		LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
		inner join users on users.id = booking.user_id	
		left JOIN COMPANY ON company.id = users.company_id
		left join gps_device_provider on gps_device_provider.id = vehicle.gps_device_provider

	join
			(
				SELECT 
					booking_commercial.id as booking_commercial_id,
					(t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
				from trip_consignment
			left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
			left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
			left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
			left join trip_events t_a on tr.id = t_a.trip_id
				where  t_a.event_id = 4
			)as t2 
		on t2.booking_commercial_id = booking_commercial.id
			
		left join 
			(SELECT booking_commercial.id as booking_commercial_id,
				t_a.event_time AS actual_delivery_date
				from trip_consignment
			left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
			left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
			left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
			left join trip_events t_a on tr.id = t_a.trip_id
			where t_a.event_id = 12
			)as t3
		on t3.booking_commercial_id = booking_commercial.id
		LEFT JOIN (SELECT EVENT_TIME, TRIP_ID
						FROM TRIP_EVENTS
						WHERE EVENT_ID = 4) AS TRIP_EVENT_START ON TRIP.ID = TRIP_EVENT_START.TRIP_ID
	left JOIN (SELECT EVENT_TIME, TRIP_ID
					FROM TRIP_EVENTS
						WHERE EVENT_ID = 12) AS TRIP_EVENT_END ON TRIP.ID = TRIP_EVENT_END.TRIP_ID
	left join (SELECT COUNT(*) AS VIOLATION_COUNT,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	group by trip.id) as total_violations on total_violations.trip_id = trip.id

	left join (SELECT COUNT(*) AS suddenStoppage,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'suddenStoppage'
	group by trip.id) as suddenStoppage on suddenStoppage.trip_id = trip.id

	left join (SELECT COUNT(*) AS harshAcceleration,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'harshAcceleration'
	group by trip.id) as harshAcceleration on harshAcceleration.trip_id = trip.id

	left join (SELECT COUNT(*) AS harshBraking,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'harshBraking'
	group by trip.id) as harshBraking on harshBraking.trip_id = trip.id

	left join (SELECT COUNT(*) AS harshManeuver,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'harshManeuver'
	group by trip.id) as harshManeuver on harshManeuver.trip_id = trip.id

	left join (SELECT COUNT(*) AS overSpeeding,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'overSpeeding'
	group by trip.id) as overSpeeding on overSpeeding.trip_id = trip.id

	left join (SELECT COUNT(*) AS nighDriving,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'nighDriving'
	group by trip.id) as nighDriving on nighDriving.trip_id = trip.id

	left join (SELECT COUNT(*) AS continuousDriving,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'continuousDriving'
	group by trip.id) as continuousDriving on continuousDriving.trip_id = trip.id

	left join (SELECT COUNT(*) AS sos,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'sos'
	group by trip.id) as sos on sos.trip_id = trip.id

	left join (SELECT COUNT(*) AS distraction,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	where detailed_violations.event_type = 'distraction'
	group by trip.id) as distraction on distraction.trip_id = trip.id
		where company.id = {} AND TRIP.TRIP_STATUS != 'Canceled') mytable {} limit {} offset {}0""".format(companyId,query_string,limit,int(offset)-1)

		cur.execute(q)
		q_result = cur.fetchall()

		totalVehicles_df = pd.DataFrame(q_result, columns = ["trip_id","vehicle_id","vehicle_number","vendor_name","driver_name","driver_number","trip_date","trip_time","trip_status","shipment_id","lr_number","last_location","last_location_date","consignor","consignee","trip_dispatch_date","booking_type","current_trip_status","trip_delivery_date","trip_eta_status","delay_duration","eta","trip_distance","distance_covered","remaining_distance","total_violations","sudden_stoppage","harsh_acceleration","harsh_braking","harsh_maneuver","overspeeding","night_driving","continuous_driving","sos","distraction"])

		totalVehicles_df = totalVehicles_df.drop_duplicates(subset = ["trip_id","vehicle_id","vehicle_number","vendor_name","driver_name","driver_number","trip_date","shipment_id","lr_number","last_location","last_location_date","consignor","consignee","trip_dispatch_date","booking_type","current_trip_status","trip_delivery_date","trip_eta_status","delay_duration","eta","trip_distance","distance_covered","remaining_distance","total_violations","sudden_stoppage","harsh_acceleration","harsh_braking","harsh_maneuver","overspeeding","night_driving","continuous_driving","sos","distraction"])

		totalVehicles_df['trip_dispatch_date'] = pd.to_datetime(totalVehicles_df['trip_dispatch_date']).dt.strftime('%d/%m/%Y')
		totalVehicles_df['trip_dispatch_date']=totalVehicles_df['trip_dispatch_date'].astype(str)

		totalVehicles_df['eta'] = pd.to_datetime(totalVehicles_df['eta']).dt.strftime('%d/%m/%Y')
		totalVehicles_df['eta']=totalVehicles_df['eta'].astype(str)

		totalVehicles_df['last_location_date'] = pd.to_datetime(totalVehicles_df['last_location_date']).dt.strftime('%d/%m/%Y')
		totalVehicles_df['last_location_date']=totalVehicles_df['last_location_date'].astype(str)

		totalVehicles_df['trip_delivery_date'] = pd.to_datetime(totalVehicles_df['trip_delivery_date']).dt.strftime('%d/%m/%Y')
		totalVehicles_df['trip_delivery_date']=totalVehicles_df['trip_delivery_date'].astype(str)

		totalVehicles_df.fillna(0)

		isDataframeEmpty = True
		if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
			isDataframeEmpty = False

		meta = {
			'totalRecords' : len(total_count.index),
			'totalPages' :  math.ceil((len(total_count.index)) / int(limit)),
			'currentPage' : int(offset),
			'currentPageCount': len(totalVehicles_df.index),
			'nextPage' : bool(isDataframeEmpty)
		}

		totalVehicles = json.loads(totalVehicles_df.to_json(orient='records'))
		json_output['data'] = totalVehicles
		json_output['meta'] = meta
	
	else:
		json_output = json.loads('{"success":"false", "message":"unsuccessful" }')

	# except Exception as e:
	# 	print(e)
	# finally:
	cur.close()
	conn.close()

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response

# VEHICLE RUNNING REPORT

@app.route('/vehicleRunningReport', methods = ['GET'])

def vehicleRunningReport():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	limit = request.args.get('perPage')
	offset = request.args.get('page')
	vendorName = request.args.get('vendorName')
	vehicleNumber = request.args.get('vehicleNumber')

	query_string = ""
	if vendorName:
		if vehicleNumber:
			query_string = "where vendor_name = '{}' and vehicle_number = '{}'".format(vendorName,vehicleNumber)
		else:
			query_string = "where vendor_name = '{}'".format(vendorName)
	elif vehicleNumber:
		query_string = "where vehicle_number = '{}'".format(vehicleNumber)

	if companyId:
		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT DISTINCT
	SHIPMENT_DETAILS.LRNO,
	VEHICLE.REGNO AS VEHICLE_NUMBER,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
	TRIP.TIME_STAMP::date AS TRIP_DATE
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
	WHERE vehicle.company_id = {} order by trip.time_stamp::date desc) mytable {}""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["lr_number","vehicle_number","vendor_name","trip_date"])

		q = """select * from (SELECT DISTINCT
	vehicle.id as vehicle_id,
	TRIP.ID AS TRIP_ID,
	TRIP.SHIPMENT_ID AS SHIPMENT_ID,
	SHIPMENT_DETAILS.LRNO,
	VEHICLE.REGNO AS VEHICLE_NUMBER,
	driver.firstname as driver_name,
	driver.phone_mobile as driver_number,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
	VEHICLE_TYPE.TYPE AS VEHICLE_TYPE,
	VEHICLE_ATTR.CAP_TON AS VEHICLE_CAPACITY,
	TRIP.TRIP_STATUS AS TRIP_STATUS,
	TRIP.TIME_STAMP::date AS TRIP_DATE,
	to_char(TRIP.TIME_STAMP::time, 'HH24:MM') AS trip_time,
	source.name as consignor,
	drops.name as consignee,
	round(TRIP.ACTUAL_DISTANCE) AS TRIP_DISTANCE,
	round(TRIP.trip_km) AS DISTANCE_COVERED,
	round(TRIP.REM_DISTANCE::decimal) AS REMAINING_DISTANCE,
	total_violations.violation_count as total_violations
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
	left join (SELECT COUNT(*) AS VIOLATION_COUNT,trip.id as trip_id
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	group by trip.id) as total_violations on total_violations.trip_id = trip.id
	WHERE vehicle.company_id = {} order by trip.time_stamp::date desc) mytable {} limit {} offset {}0""".format(companyId,query_string,limit,int(offset)-1)

		cur.execute(q)
		q_result = cur.fetchall()

		totalVehicles_df = pd.DataFrame(q_result, columns = ["vehicle_id","trip_id","shipment_id","lrno","vehicle_number","driver_name","driver_number","vendor_name","vehicle_type","vehicle_capacity","trip_status","trip_date","trip_time","consignor","consignee","trip_distance","distance_covered","remaining_distance","total_violations"])

		totalVehicles_df = totalVehicles_df.drop_duplicates(subset = ["vehicle_id"])

		totalVehicles_df['trip_date'] = pd.to_datetime(totalVehicles_df['trip_date']).dt.strftime('%d/%m/%Y')
		totalVehicles_df['trip_date']=totalVehicles_df['trip_date'].astype(str)

		totalVehicles_df['trip_date'] = totalVehicles_df['trip_date'].replace("nan","Not on trip")

		isDataframeEmpty = True
		if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
			isDataframeEmpty = False

		meta = {
			'totalRecords' : len(total_count.index),
			'totalPages' :  math.ceil((len(total_count.index)) / int(limit)),
			'currentPage' : int(offset),
			'currentPageCount': len(totalVehicles_df.index),
			'nextPage' : bool(isDataframeEmpty)
		}

		totalVehicles = json.loads(totalVehicles_df.to_json(orient='records'))
		json_output['data'] = totalVehicles
		json_output['meta'] = meta

	else:
		json_output = json.loads('{"success":"false", "message":"unsuccessful" }')

	# except Exception as e:
	# 	print(e)
	# finally:
	cur.close()
	conn.close()

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response

# ALERT SUMMARY REPORT

@app.route('/alertSummaryReport', methods = ['GET'])

def alertSummaryReport():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	limit = request.args.get('perPage')
	offset = request.args.get('page')
	vendorName = request.args.get('vendorName')
	vehicleNumber = request.args.get('vehicleNumber')

	query_string = ""
	if vendorName:
		if vehicleNumber:
			query_string = "where vendor_name = '{}' and vehicle_number = '{}'".format(vendorName,vehicleNumber)
		else:
			query_string = "where vendor_name = '{}'".format(vendorName)
	elif vehicleNumber:
		query_string = "where vehicle_number = '{}'".format(vehicleNumber)

	if companyId:
		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT DISTINCT
	VEHICLE.REGNO AS VEHICLE_NUMBER,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
	WHERE vehicle.company_id = {}) mytable {}""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["vehicle_number","vendor_name"])

		q = """select * from (SELECT DISTINCT
	vehicle.id as vehicle_id,
	VEHICLE.REGNO AS VEHICLE_NUMBER,
	driver.firstname as driver_name,
	driver.phone_mobile as driver_number,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
	VEHICLE_TYPE.TYPE AS VEHICLE_TYPE,
	VEHICLE_ATTR.CAP_TON AS VEHICLE_CAPACITY,
	total_violations.violation_count as total_violations,
	suddenStoppage.suddenStoppage as sudden_stoppage,
	harshAcceleration.harshAcceleration as harsh_acceleration,
	harshBraking.harshBraking as harsh_braking,
	harshManeuver.harshManeuver as harsh_maneuver,
	overSpeeding.overSpeeding as overSpeeding,
	nighDriving.nighDriving as night_driving,
	continuousDriving.continuousDriving as continuous_driving,
	sos.sos as sos,
	distraction.distraction as distraction
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
			   
	left join (SELECT COUNT(*) AS VIOLATION_COUNT,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
	group by imei) as total_violations on total_violations.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS suddenStoppage,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'suddenStoppage'
	group by imei) as suddenStoppage on suddenStoppage.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS harshAcceleration,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'harshAcceleration'
	group by imei) as harshAcceleration on harshAcceleration.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS harshBraking,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'harshBraking'
	group by imei) as harshBraking on harshBraking.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS harshManeuver,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'harshManeuver'
	group by imei) as harshManeuver on harshManeuver.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS overSpeeding,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'overSpeeding'
	group by imei) as overSpeeding on overSpeeding.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS nighDriving,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'nighDriving'
	group by imei) as nighDriving on nighDriving.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS continuousDriving,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'continuousDriving'
	group by imei) as continuousDriving on continuousDriving.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS sos,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'sos'
	group by imei) as sos on sos.vehicle_number = vehicle.regno
			   
	left join (SELECT COUNT(*) AS distraction,imei as vehicle_number
	FROM DETAILED_VIOLATIONS
	left join vehicle on vehicle.regno = detailed_violations.imei
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join trip on trip.vehicle_booking_details = vehicle_booking_details.id
			   where detailed_violations.event_type = 'distraction'
	group by imei) as distraction on distraction.vehicle_number = vehicle.regno
	WHERE vehicle.company_id = {}) mytable {} limit {} offset {}0""".format(companyId,query_string,limit,int(offset)-1)

		cur.execute(q)
		q_result = cur.fetchall()

		totalVehicles_df = pd.DataFrame(q_result, columns = ["vehicle_id","vehicle_number","driver_name","driver_number","vendor_name","vehicle_type","vehicle_capacity","total_violations","sudden_stoppage","harsh_acceleration","harsh_braking","harsh_maneuver","overspeeding","night_driving","continuous_driving","sos","distraction"])

		totalVehicles_df = totalVehicles_df.drop_duplicates(subset = ["vehicle_id"])

		isDataframeEmpty = True
		if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
			isDataframeEmpty = False

		meta = {
			'totalRecords' : len(total_count.index),
			'totalPages' :  math.ceil((len(total_count.index)) / int(limit)),
			'currentPage' : int(offset),
			'currentPageCount': len(totalVehicles_df.index),
			'nextPage' : bool(isDataframeEmpty)
		}

		totalVehicles = json.loads(totalVehicles_df.to_json(orient='records'))
		json_output['data'] = totalVehicles
		json_output['meta'] = meta

	else:
		json_output = json.loads('{"success":"false", "message":"unsuccessful" }')

	# except Exception as e:
	# 	print(e)
	# finally:
	cur.close()
	conn.close()

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response

# DRIVER PERFORMANCE

@app.route('/driverPerformance', methods = ['GET'])

def driverPerformanceReport():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	limit = request.args.get('perPage')
	offset = request.args.get('page')
	vendorName = request.args.get('vendorName')
	driverName = request.args.get('driverName')
	# vehicleNumber = request.args.get('vehicleNumber')

	query_string = ""
	if vendorName:
		if driverName:
			query_string = "where vendor_name = '{}' and driver_id = '{}'".format(vendorName,driverName)
		else:
			query_string = "where vendor_name = '{}'".format(vendorName)
	elif driverName:
		query_string = "where driver_id = '{}'".format(driverName)

	if companyId:
		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT distinct driver.id as driver_id,DRIVER.FIRSTNAME AS DRIVER_NAME,
	VEHICLE.REGNO AS CURRENT_VEHICLE_NUMBER,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	inner JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
where vehicle.company_id = {}) mytable {}""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["driver_id","driver_name","vehicle_number","vendor_name"])

		q = """select * from (SELECT distinct driver.id as driver_id,DRIVER.FIRSTNAME AS DRIVER_NAME,
	DRIVER.PHONE_MOBILE AS DRIVER_NUMBER,
	VEHICLE.REGNO AS CURRENT_VEHICLE_NUMBER,
	VIOLATIONS.VIOLATIONS AS TOTAL_ALERTS,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
	TOTAL_VEHICLES_DRIVEN.TOTAL_VEHICLES AS TOTAL_VEHICLES_DRIVEN,
	(TOTAL_DISTANCE.ON_TIME + TOTAL_DISTANCE.DELAY) as total_trips,
	ROUND(TOTAL_DISTANCE.TOTAL_DISTANCE)::integer AS TOTAL_DISTANCE,
	TOTAL_DISTANCE.ON_TIME AS ON_TIME_TRIPS,
	TOTAL_DISTANCE.DELAY AS DELAYED_TRIPS,
	AVG_VIOLATIONS.AVG_VIOLATIONS::integer AS AVG_VIOLATIONS_PER_TRIP,
	TOTAL_TRANSPORTERS.TOTAL_TRANSPORTERS AS NO_OF_TRANSPORTERS_CATERED_TO
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	inner JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
	LEFT JOIN
		(SELECT imei,
				COUNT(*) AS VIOLATIONS
			FROM DETAILED_VIOLATIONS
			GROUP BY imei) AS VIOLATIONS ON VIOLATIONS.imei = VEHICLE.regno
	LEFT JOIN
		(SELECT VEHICLEID,
				COUNT(VEHICLEID) AS TOTAL_VEHICLES
			FROM DRIVER
			GROUP BY VEHICLEID) AS TOTAL_VEHICLES_DRIVEN ON TOTAL_VEHICLES_DRIVEN.VEHICLEID = VEHICLE.ID
	LEFT JOIN 
		(SELECT driver.id as driver_id,driver.firstname AS driver_NAME,
			COUNT(TRIP.ID) AS TOTAL_TRIPS,
			SUM(TRIP.ACTUAL_DISTANCE) AS TOTAL_DISTANCE,
		count(trip.current_eta-trip.actual_eta > (INTERVAL '0 hours 0 minutes')) as on_time,
		(count(trip.id) - count(trip.current_eta-trip.actual_eta > (INTERVAL '0 hours 0 minutes'))) as delay
		FROM TRIP
		JOIN VEHICLE_BOOKING_DETAILS ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
		JOIN VEHICLE ON VEHICLE.ID = VEHICLE_BOOKING_DETAILS.VEHICLE_ID
		join driver on driver.vehicleid = vehicle.id
		GROUP BY driver.firstname,driver.id) as total_distance on total_distance.driver_id = driver.id
	LEFT JOIN(
		SELECT driver.id as driver_id,DRIVER.FIRSTNAME AS DRIVER_NAME,
	COUNT(VENDOR.ID) AS TOTAL_TRANSPORTERS
FROM VEHICLE
JOIN VENDOR_VEHICLE ON VENDOR_VEHICLE.VEHICLE_ID = VEHICLE.ID
JOIN VENDOR ON VENDOR_VEHICLE.VENDOR_ID = VENDOR.ID
JOIN DRIVER ON DRIVER.VEHICLEID = VEHICLE.ID
GROUP BY DRIVER.FIRSTNAME,driver.id) as total_transporters on total_transporters.driver_id = driver.id
left join (SELECT DISTINCT vehicle.ID AS vehicle_id, count(detailed_violations.event_type) as avg_violations
FROM vehicle
inner join detailed_violations on detailed_violations.imei = vehicle.regno
group by vehicle.id) as avg_violations on avg_violations.vehicle_id = vehicle.id
where vehicle.company_id = {}) mytable {} limit {} offset {}0""".format(companyId,query_string,limit,int(offset)-1)

		cur.execute(q)
		q_result = cur.fetchall()

		totalVehicles_df = pd.DataFrame(q_result, columns = ["driver_id","driver_name","driver_number","current_vehicle_number","total_alerts","vendor_name","total_vehicles_driven","total_trips","total_distance","on_time_trips","delayed_trips","avg_violations_per_trip","no_of_transporters_catered_to"])

		totalVehicles_df = totalVehicles_df.drop_duplicates(subset = ["driver_name"])

		isDataframeEmpty = True
		if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
			isDataframeEmpty = False

		meta = {
			'totalRecords' : len(total_count.index),
			'totalPages' :  math.ceil((len(total_count.index)) / int(limit)),
			'currentPage' : int(offset),
			'currentPageCount': len(totalVehicles_df.index),
			'nextPage' : bool(isDataframeEmpty)
		}

		totalVehicles = json.loads(totalVehicles_df.to_json(orient='records'))
		json_output['data'] = totalVehicles
		json_output['meta'] = meta

	else:
		json_output = json.loads('{"success":"false", "message":"unsuccessful" }')

	# except Exception as e:
	# 	print(e)
	# finally:
	cur.close()
	conn.close()

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response

# TRANSPORTER PERFORMANCE

@app.route('/transporterPerformance', methods = ['GET'])

def transporterPerformance():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	limit = request.args.get('perPage')
	offset = request.args.get('page')
	vendorName = request.args.get('vendorName')

	query_string = ""
	if vendorName:
		query_string = "where vendor_name = '{}'".format(vendorName)

	if companyId:
		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT DISTINCT
		VENDOR.VENDOR_NAME AS vendor_name
	FROM VENDOR
	JOIN VENDOR_VEHICLE ON VENDOR_VEHICLE.VENDOR_ID = VENDOR.ID
	JOIN COMPANY ON VENDOR.COMPANY_ID = COMPANY.ID
	 where company.id = {}) mytable {}""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["vendor_name"])

		q = """select * from (SELECT DISTINCT
		case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
		TOTAL_VEHICLES.TOTAL_VEHICLES AS TOTAL_VEHICLES,
		TOTAL_TRIPS.TOTAL_TRIPS AS TOTAL_TRIPS,
		round(TOTAL_TRIPS.TOTAL_DISTANCE) AS TOTAL_DISTANCE,
		TOTAL_VIOLATIONS.TOTAL_VIOLATIONS AS TOTAL_VIOLATIONS,
		'' AS total_routes_covered,
		round(total_trips.avg_dist_per_day) AS avg_dist_per_day,
		'' as avg_driving_time,
		'' as avg_stoppage_time,
		total_trips.on_time as on_time_trips,
		total_trips.delay as delayed_trips,
		avg_violations.avg_violations as avg_violations_per_trip,
		total_trips.avg_trips as avg_trips_per_month
	FROM VENDOR
	JOIN VENDOR_VEHICLE ON VENDOR_VEHICLE.VENDOR_ID = VENDOR.ID
	JOIN COMPANY ON VENDOR.COMPANY_ID = COMPANY.ID
	JOIN VEHICLE ON VENDOR_VEHICLE.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE.ID = VEHICLE_BOOKING_DETAILS.VEHICLE_ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN
		(SELECT VENDOR_VEHICLE.VENDOR_ID AS VENDOR_ID,
				COUNT(VENDOR_VEHICLE.VEHICLE_ID) AS TOTAL_VEHICLES
			FROM VENDOR_VEHICLE
			GROUP BY VENDOR_VEHICLE.VENDOR_ID) AS TOTAL_VEHICLES ON TOTAL_VEHICLES.VENDOR_ID = VENDOR.ID
	LEFT JOIN
		(SELECT VENDOR.VENDOR_NAME AS VENDOR_NAME,
				COUNT(TRIP.ID) AS TOTAL_TRIPS,
				SUM(TRIP.ACTUAL_DISTANCE) AS TOTAL_DISTANCE,
				count(trip.current_eta-trip.actual_eta > (INTERVAL '0 hours 0 minutes')) as on_time,
		(count(trip.id) - count(trip.current_eta-trip.actual_eta > (INTERVAL '0 hours 0 minutes'))) as delay,
		(avg(trip.actual_distance)) as avg_dist_per_day,
		(count(trip.id)/count(vendor.id)) as avg_trips
			FROM TRIP
			JOIN VEHICLE_BOOKING_DETAILS ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
			JOIN VEHICLE ON VEHICLE.ID = VEHICLE_BOOKING_DETAILS.VEHICLE_ID
			JOIN VENDOR_VEHICLE ON VENDOR_VEHICLE.VEHICLE_ID = VEHICLE.ID
			JOIN VENDOR ON VENDOR.ID = VENDOR_VEHICLE.VENDOR_ID
			GROUP BY(VENDOR.VENDOR_NAME)) AS TOTAL_TRIPS ON TOTAL_TRIPS.VENDOR_NAME = VENDOR.VENDOR_NAME
	LEFT JOIN
		(SELECT VENDOR.VENDOR_NAME,
				COUNT(VEHICLE_VIOLATIONS.EVENT_TYPE) AS TOTAL_VIOLATIONS
			FROM VENDOR
			JOIN VENDOR_VEHICLE ON VENDOR_VEHICLE.VENDOR_ID = VENDOR.ID
			JOIN VEHICLE_VIOLATIONS ON VEHICLE_VIOLATIONS.VEHICLE_ID = VENDOR_VEHICLE.VEHICLE_ID
			GROUP BY(VENDOR.VENDOR_NAME)) AS TOTAL_VIOLATIONS ON TOTAL_VIOLATIONS.VENDOR_NAME = VENDOR.VENDOR_NAME
	left join (SELECT DISTINCT TRIP.ID AS TRIP_ID, count(vehicle_violations.event_type) as avg_violations
FROM TRIP
inner join vehicle_violations on vehicle_violations.trip_id = trip.id
group by trip.id) as avg_violations on avg_violations.trip_id = trip.id where company.id = {}) mytable {} limit {} offset {}0""".format(companyId,query_string,limit,int(offset)-1)

		cur.execute(q)
		q_result = cur.fetchall()

		totalVehicles_df = pd.DataFrame(q_result, columns = ["vendor_name","total_vehicles","total_trips","total_distance","total_violations","total_routes_covered","avg_dist_per_day","avg_driving_time","avg_stoppage_time","on_time_trips","delayed_trips","avg_violations_per_trip","avg_trips_per_month"])

		totalVehicles_df = totalVehicles_df.drop_duplicates(subset = ["vendor_name"])

		isDataframeEmpty = True
		if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
			isDataframeEmpty = False

		meta = {
			'totalRecords' : len(total_count.index),
			'totalPages' :  math.ceil((len(total_count.index)) / int(limit)),
			'currentPage' : int(offset),
			'currentPageCount': len(totalVehicles_df.index),
			'nextPage' : bool(isDataframeEmpty)
		}

		totalVehicles = json.loads(totalVehicles_df.to_json(orient='records'))
		json_output['data'] = totalVehicles
		json_output['meta'] = meta

	else:
		json_output = json.loads('{"success":"false", "message":"unsuccessful" }')

	# except Exception as e:
	# 	print(e)
	# finally:
	cur.close()
	conn.close()

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response


#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------ANALYTICS----------------------------------------------------------

#TOTAL VEHICLES ON TRIP

@app.route('/totalVehiclesOnTrip', methods = ['GET'])

def totalVehiclesOnTrip():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	tripType = request.args.get('bookingType')
	limit = request.args.get('perPage')
	offset = request.args.get('page')


	if companyId:
		query_string = ""
		if tripType:
			query_string = "and booking_commercial.logistic_booking_type = '{}'".format(tripType)

		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select count(distinct 
	SHIPMENT_DETAILS.LRNO) AS trip_count,
	case 
			when t2.eta-t3.actual_delivery_date > (INTERVAL '0 hours 0 minutes') 
			then 'Delayed' 
			when t2.eta-t3.actual_delivery_date < (INTERVAL '0 hours 0 minutes') then 'Arriving early' else 'On Time'
			end as trip_eta_status,
	booking_commercial.logistic_booking_type as booking_type
		from
		trip_track
		join trip on trip.id = trip_track.trip_id
		join shipment_details on shipment_details.drop_id = trip_track.drop_id
		join drops on drops.id = trip_track.drop_id

		join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
		join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
		left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
		left join vehicle_status on vehicle_status.vehicle_id = vehicle.id
		join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
		join vehicle_type on vehicle_attr. vehicle_type_id = vehicle_type.id
		join booking on booking.id = vehicle_booking_details.booking_id
		inner join users on users.id = booking.user_id	
		left JOIN COMPANY ON company.id = users.company_id	
		join
			(
				SELECT 
					booking_commercial.id as booking_commercial_id,
					(t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
				from trip_consignment
			left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
			left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
			left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
			left join trip_events t_a on tr.id = t_a.trip_id
				where  t_a.event_id = 4
			)as t2 
		on t2.booking_commercial_id = booking_commercial.id
			
		left join 
			(SELECT booking_commercial.id as booking_commercial_id,
				t_a.event_time AS actual_delivery_date
				from trip_consignment
			left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
			left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
			left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
			left join trip_events t_a on tr.id = t_a.trip_id
			where t_a.event_id = 12
			)as t3
		on t3.booking_commercial_id = booking_commercial.id
		where company.id = {} AND TRIP.TRIP_STATUS = 'Intransit' {}
			  group by booking_commercial.logistic_booking_type,t3.actual_delivery_date,t2.eta
		""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["trip_count","eta_status","booking_type"])

		on_time_df = total_count[(total_count['eta_status'] == 'On Time')]  
		delay_df = total_count[(total_count['eta_status'] == 'Delayed')] 

		json_output['on_time']=json.loads(on_time_df.to_json(orient='records'))
		json_output['delay']=json.loads(delay_df.to_json(orient='records'))
		json_output['booking_type']=json.loads(total_count['booking_type'].to_json(orient='records'))

		if limit and offset:
			vehicle_plan_query = """select * from (select distinct trip.id,vehicle.id as vehicle_id,
	vehicle.regno as vehicle_number,
	VEHICLE_TYPE.TYPE AS VEHICLE_TYPE,
	VEHICLE_TYPE.MAX_TON_CAPACITY AS VEHICLE_CAPACITY,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null 
	then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
	else company.company_name end as vendor_name,
	driver.firstname as driver_name,
	driver.phone_mobile as driver_number,
	trip.time_stamp::date as booking_date,
	trip.shipment_id as shipment_id,
	SHIPMENT_DETAILS.LRNO AS LR_NO,
	concat(vehicle_status.lattitude,',',vehicle_status.longitude) as last_location,
	vehicle_status.location_update_time::date as last_location_date,
	source.name as consignor,
	drops.name as consignee,
	TRIP_EVENT_START.event_time::date as trip_dispatch_date,
	booking_commercial.logistic_booking_type as booking_type,
	trip.TRIP_STATUS AS CURRENT_TRIP_STATUS,
	case 
			when t2.eta-t3.actual_delivery_date > (INTERVAL '0 hours 0 minutes') 
			then 'On Time' 
			when t2.eta-t3.actual_delivery_date < (INTERVAL '0 hours 0 minutes') then 'Arriving early' else 'On Time'
			end as trip_eta_status,
	trip.current_eta::date-trip.actual_eta::date as delay_duration,
	trip.current_eta::date as eta
		from
		trip_track
		join trip on trip.id = trip_track.trip_id
		join shipment_details on shipment_details.drop_id = trip_track.drop_id
		join drops on drops.id = trip_track.drop_id

		join trip_consignment on trip_track.drop_id = trip_consignment.drop_id
		join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id
		left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
		join vehicle on vehicle_booking_details.vehicle_id = vehicle.id
		left join vehicle_status on vehicle_status.vehicle_id = vehicle.id
		join vehicle_attr on vehicle_attr.vehicle_id = vehicle.id
		join vehicle_type on vehicle_attr. vehicle_type_id = vehicle_type.id
		join booking on booking.id = vehicle_booking_details.booking_id
		left join source on booking.source_id = source.id
		LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
		inner join users on users.id = booking.user_id	
		left JOIN COMPANY ON company.id = users.company_id
		left join gps_device_provider on gps_device_provider.id = vehicle.gps_device_provider

	join
			(
				SELECT 
					booking_commercial.id as booking_commercial_id,
					(t_a.time_stamp::timestamp::date) + make_interval(days => booking_commercial.customer_tat) AS eta
				from trip_consignment
			left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
			left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
			left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
			left join trip_events t_a on tr.id = t_a.trip_id
				where  t_a.event_id = 4
			)as t2 
		on t2.booking_commercial_id = booking_commercial.id
			
		left join 
			(SELECT booking_commercial.id as booking_commercial_id,
				t_a.event_time AS actual_delivery_date
				from trip_consignment
			left join booking_commercial on booking_commercial.trip_consignment_id = trip_consignment.trip_consignment_id	
			left join vehicle_booking_details on vehicle_booking_details.id = trip_consignment.vehicle_booking_details_id
			left join trip tr on tr.vehicle_booking_details = vehicle_booking_details.id	
			left join trip_events t_a on tr.id = t_a.trip_id
			where t_a.event_id = 12
			)as t3
		on t3.booking_commercial_id = booking_commercial.id
		LEFT JOIN (SELECT EVENT_TIME, TRIP_ID
						FROM TRIP_EVENTS
						WHERE EVENT_ID = 4) AS TRIP_EVENT_START ON TRIP.ID = TRIP_EVENT_START.TRIP_ID
	left JOIN (SELECT EVENT_TIME, TRIP_ID
					FROM TRIP_EVENTS
						WHERE EVENT_ID = 12) AS TRIP_EVENT_END ON TRIP.ID = TRIP_EVENT_END.TRIP_ID
	
		where company.id = {} AND TRIP.TRIP_STATUS = 'Intransit' {}) mytable limit {} offset {}""".format(companyId,query_string,limit,(int(offset)-1)*int(limit))
			cur.execute(vehicle_plan_query)
			vehicle_plan_query_result = cur.fetchall()
			vehicle_planning = pd.DataFrame(vehicle_plan_query_result, columns = ["id","vehicle_id","vehicle_number","vehicle_type","vehicle_capacity","vendor_name","driver_name","driver_number","booking_date","shipment_id","lr_no","last_location","last_location_date","consignor","consignee","trip_dispatch_date","booking_type","current_trip_status","trip_eta_status","delay_duration","eta"])
			
			vehicle_planning['last_location_date'] = pd.to_datetime(vehicle_planning['last_location_date']).dt.strftime('%d/%m/%Y')
			vehicle_planning['last_location_date']=vehicle_planning['last_location_date'].astype(str)
			
			vehicle_planning['eta'] = pd.to_datetime(vehicle_planning['eta']).dt.strftime('%d/%m/%Y')
			vehicle_planning['eta']=vehicle_planning['eta'].astype(str)
			
			vehicle_planning['trip_dispatch_date'] = pd.to_datetime(vehicle_planning['trip_dispatch_date']).dt.strftime('%d/%m/%Y')
			vehicle_planning['trip_dispatch_date']=vehicle_planning['trip_dispatch_date'].astype(str)

			vehicle_planning['booking_date'] = pd.to_datetime(vehicle_planning['booking_date']).dt.strftime('%d/%m/%Y')
			vehicle_planning['booking_date']=vehicle_planning['booking_date'].astype(str)

			vehicle_planning = vehicle_planning.replace("nan", "0")
			vehicle_planning = vehicle_planning.replace("NaT", "N/A")
			#vehicle_planning = vehicle_planning.drop_duplicates(subset = ['shipment_id'])

			print(vehicle_planning)
			
			for i in vehicle_planning.index:
				try:
					url = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + str(vehicle_planning.loc[i,'last_location']) + "&key=AIzaSyBjE1eaTyn8ELbunCIvrvGg22gHI-JktG0"
					response = requests.request("GET",url) 
					json_res = json.loads(str(response.text))
					#print("@@@@@@@@@@@@@@@",json_res['results'][0]['formatted_address'])
					vehicle_planning.loc[i,'last_location'] = json_res['results'][0]['formatted_address']
				except:
					vehicle_planning.loc[i,'last_location'] = "Not Available"

			
			on_trip_df = vehicle_planning[(vehicle_planning['trip_eta_status'] == 'On Time')]  
			off_trip_df = vehicle_planning[(vehicle_planning['trip_eta_status'] == 'Delayed')]  

			isDataframeEmpty = True
			if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
				isDataframeEmpty = False

			meta = {
				'totalRecords' : int(total_count['trip_count'].sum()),
				'totalPages' :  math.ceil(int(len(total_count.index)) / int(limit)),
				'currentPage' : int(offset),
				'currentPageCount': len(vehicle_planning.index),
				'nextPage' : bool(isDataframeEmpty)
			}

			json_output['on_time']=json.loads(on_trip_df.to_json(orient='records'))
			json_output['delay']=json.loads(off_trip_df.to_json(orient='records'))
			json_output['meta'] = meta

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response


# VEHICLE AVAILABILITY 

@app.route('/vehicleAvailability', methods = ['GET'])

def vehicleAvailability():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	limit = request.args.get('perPage')
	offset = request.args.get('page')

	if companyId:
		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT count(DISTINCT
	vehicle.id) as vehicle_id	
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID	
	WHERE vehicle.company_id = {} ) mytable 
		""".format(companyId)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["vehicle_count"])
		print(total_count)
		json_output['data']=json.loads(total_count.to_json(orient='records'))

		if limit and offset:
			vehicle_plan_query = """select * from (SELECT DISTINCT
	vehicle.id as vehicle_id,
			   BRANCH.branch_name as branch_name,
	VEHICLE.REGNO AS VEHICLE_NUMBER,
			   VEHICLE_TYPE.TYPE AS VEHICLE_TYPE,
	VEHICLE_TYPE.MAX_TON_CAPACITY AS VEHICLE_CAPACITY,
	driver.firstname as driver_name,
	driver.phone_mobile as driver_number,
	case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
	COALESCE(TRIP_EVENT_END.EVENT_TIME::date, now()::date) AS AVAILABLE_SINCE,
			   concat(vehicle_status.lattitude,',',vehicle_status.longitude) as last_location,
		vehicle_status.location_update_time::date as last_location_date
	FROM VEHICLE
	LEFT JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
	LEFT JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
	LEFT JOIN TRIP_TRACK ON TRIP_TRACK.TRIP_ID = TRIP.ID
	LEFT JOIN VEHICLE_STATUS ON VEHICLE.ID = VEHICLE_STATUS.VEHICLE_ID
	LEFT JOIN VEHICLE_ATTR ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE_ATTR.VEHICLE_ID
	LEFT JOIN VEHICLE_TYPE ON VEHICLE_TYPE.ID = VEHICLE_ATTR.VEHICLE_TYPE_ID
	LEFT JOIN TRIP_CONSIGNMENT ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
	LEFT JOIN BOOKING_COMMERCIAL ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
	LEFT JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
	LEFT JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
	LEFT JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
	LEFT JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
	LEFT JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
	LEFT JOIN DRIVER ON VEHICLE.DRIVER_ID = DRIVER.ID
	LEFT JOIN USERS ON USERS.ID = BOOKING.USER_ID
	LEFT JOIN COMPANY ON COMPANY.ID = USERS.COMPANY_ID
			   LEFT JOIN (SELECT EVENT_TIME, TRIP_ID
					FROM TRIP_EVENTS
		   			WHERE EVENT_ID = 12) AS TRIP_EVENT_END ON TRIP.ID = TRIP_EVENT_END.TRIP_ID
	WHERE vehicle.company_id = {} ) mytable limit {} offset {}""".format(companyId,limit,(int(offset)-1)*int(limit))
			cur.execute(vehicle_plan_query)
			vehicle_plan_query_result = cur.fetchall()

			vehicle_planning = pd.DataFrame(vehicle_plan_query_result, columns = ["vehicle_id","branch_name","vehicle_number","vehicle_type","vehicle_capacity","driver_name","driver_number","vendor_name","available_since","last_location","last_location_date"])
		
			
			vehicle_planning['last_location_date'] = pd.to_datetime(vehicle_planning['last_location_date']).dt.strftime('%d/%m/%Y')
			vehicle_planning['last_location_date']=vehicle_planning['last_location_date'].astype(str)
			
			vehicle_planning['eta'] = pd.to_datetime(vehicle_planning['eta']).dt.strftime('%d/%m/%Y')
			vehicle_planning['eta']=vehicle_planning['eta'].astype(str)
			
			vehicle_planning['trip_dispatch_date'] = pd.to_datetime(vehicle_planning['trip_dispatch_date']).dt.strftime('%d/%m/%Y')
			vehicle_planning['trip_dispatch_date']=vehicle_planning['trip_dispatch_date'].astype(str)

			vehicle_planning = vehicle_planning.replace("nan", "0")

			vehicle_planning = vehicle_planning.replace("NaT", "N/A")
			vehicle_planning = vehicle_planning.drop_duplicates(subset = ['vehicle_number'])

			vehicle_planning['time_band'] = ''
			vehicle_planning['distance_from_consignor'] = ''
			vehicle_planning['previous_trip_consignee_name'] = ''
			vehicle_planning['previous_trip_consignor_name'] = ''
			vehicle_planning['previous_trip_lr_number'] = ''
			vehicle_planning['previous_trip_dispatch_date'] = ''
			vehicle_planning['previous_trip_delivery_date'] = ''

			json_output = json.loads('{"data":[],"success":"true", "message":"success" }')
			
			for i in vehicle_planning.index:
				previous_trip_details = """ SELECT VEHICLE.ID,
			DROPS.plant AS PREVIOUS_CONSIGNEE,
			SOURCE.plant AS PREVIOUS_CONSIGNOR,
			SHIPMENT_DETAILS.CUSTOMER_LR_NUMBER AS PREVIOUS_TRIP_LRNO,
			TRIP_EVENT_START.EVENT_TIME::date AS PREVIOUS_TRIP_DISPATCH_DATE,
			TRIP_EVENT_END.EVENT_TIME::date AS PREVIOUS_TRIP_DELIVERY_DATE
			FROM BOOKING_COMMERCIAL
			
			JOIN TRIP_CONSIGNMENT ON BOOKING_COMMERCIAL.TRIP_CONSIGNMENT_ID = TRIP_CONSIGNMENT.TRIP_CONSIGNMENT_ID
			INNER JOIN CUSTOMER_LR_NUMBERS ON TRIP_CONSIGNMENT.CUSTOMER_LR_NUMBERS_ID = CUSTOMER_LR_NUMBERS.ID
			INNER JOIN VEHICLE_BOOKING_DETAILS ON VEHICLE_BOOKING_DETAILS.ID = TRIP_CONSIGNMENT.VEHICLE_BOOKING_DETAILS_ID
			INNER JOIN VEHICLE ON VEHICLE_BOOKING_DETAILS.VEHICLE_ID = VEHICLE.ID
			INNER JOIN company ON vehicle.company_id = company.ID
			INNER JOIN VEHICLE_STATUS ON VEHICLE_STATUS.VEHICLE_ID = VEHICLE.ID
			INNER JOIN VEHICLE_ATTR ON VEHICLE_ATTR.VEHICLE_ID = VEHICLE.ID
			INNER JOIN VEHICLE_TYPE ON VEHICLE_ATTR.VEHICLE_TYPE_ID = VEHICLE_TYPE.ID
			INNER JOIN TRIP ON TRIP.VEHICLE_BOOKING_DETAILS = VEHICLE_BOOKING_DETAILS.ID
			INNER JOIN TRIP_TRACK ON TRIP.ID = TRIP_TRACK.TRIP_ID
			INNER JOIN BOOKING ON BOOKING.ID = VEHICLE_BOOKING_DETAILS.BOOKING_ID
			INNER JOIN SOURCE ON BOOKING.SOURCE_ID = SOURCE.ID
			INNER JOIN DROPS ON BOOKING.FINAL_DROP_ID = DROPS.ID
			INNER JOIN SHIPMENT_DETAILS ON BOOKING.FINAL_DROP_ID = SHIPMENT_DETAILS.DROP_ID
			INNER JOIN BRANCH ON BRANCH.ID = BOOKING.BRANCH_ID
			LEFT JOIN (SELECT EVENT_TIME, TRIP_ID
					FROM TRIP_EVENTS
					WHERE EVENT_ID = 8) AS TRIP_EVENT_START ON TRIP.ID = TRIP_EVENT_START.TRIP_ID
			LEFT JOIN (SELECT EVENT_TIME, TRIP_ID
					FROM TRIP_EVENTS
					WHERE EVENT_ID = 12) AS TRIP_EVENT_END ON TRIP.ID = TRIP_EVENT_END.TRIP_ID
			WHERE vehicle.ID = '{}' order by TRIP.ID desc LIMIT 1""".format(vehicle_planning.loc[i,"vehicle_id"])
				cur.execute(previous_trip_details)
				previous_trip_details_result = cur.fetchall()
				previous_trip = pd.DataFrame(previous_trip_details_result, columns = ['vehicle_id','previous_trip_consignee_name', 'previous_trip_consignor_name','previous_trip_lr_number','previous_trip_dispatch_date','previous_trip_delivery_date'])


				previous_trip['previous_trip_dispatch_date'] = pd.to_datetime(previous_trip['previous_trip_dispatch_date']).dt.strftime('%d/%m/%Y')
				previous_trip['previous_trip_dispatch_date']=previous_trip['previous_trip_dispatch_date'].astype(str)

				previous_trip['previous_trip_delivery_date'] = pd.to_datetime(previous_trip['previous_trip_delivery_date']).dt.strftime('%d/%m/%Y')
				previous_trip['previous_trip_delivery_date']=previous_trip['previous_trip_delivery_date'].astype(str)
				
				previous_trip = previous_trip.replace("nan", "")

				try:
					url = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + str(vehicle_planning.loc[i,'last_location']) + "&key=AIzaSyBjE1eaTyn8ELbunCIvrvGg22gHI-JktG0"
					response = requests.request("GET",url) 
					json_res = json.loads(str(response.text))
					#print("@@@@@@@@@@@@@@@",json_res['results'][0]['formatted_address'])
					vehicle_planning.loc[i,'last_location'] = json_res['results'][0]['formatted_address']
				except:
					vehicle_planning.loc[i,'last_location'] = "Not Available"


			isDataframeEmpty = True
			if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
				isDataframeEmpty = False

			meta = {
				'totalRecords' : int(len(total_count.index)),
				'totalPages' :  math.ceil(int(len(total_count.index)) / int(limit)),
				'currentPage' : int(offset),
				'currentPageCount': len(vehicle_planning.index),
				'nextPage' : bool(isDataframeEmpty)
			}

			json_output['data']=json.loads(vehicle_planning.to_json(orient='records'))
			json_output['meta'] = meta

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)
	
	return response


#TOP BOTTOM TRANSPORTERS

@app.route('/top_bottom_transporters', methods = ['GET'])

def top_bottom_transporters():
	conn = psycopg2.connect(host="127.0.0.1", port = 5432, database="ezyloads", user="ezyloads", password="ezy@1234")
	cur = conn.cursor()

	companyId = request.args.get('companyId')
	vendorName = request.args.get('vendorName')
	limit = request.args.get('perPage')
	offset = request.args.get('page')

	if companyId:
		query_string = ""
		if vendorName:
			query_string = "and vendor_name = '{}'".format(vendorName)

		json_output = json.loads('{"success":"true", "message":"success" }')

		total_records = """select * from (SELECT distinct trip_count.trip_id as trip_counts,case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null 
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name
	FROM VENDOR
	left join vendor_vehicle on vendor.id = vendor_vehicle.vendor_id
	left join vehicle on vendor_vehicle.vehicle_id = vehicle.id
	left join company on vehicle.company_id = company.id
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join (select count(trip.id) as trip_id, vendor.vendor_name from trip
			join vehicle_booking_details on trip.vehicle_booking_details = vehicle_booking_details.id
			join vehicle on vehicle.id = vehicle_booking_details.vehicle_id
			join vendor_vehicle on vendor_vehicle.vehicle_id = vehicle.id
			join vendor on vendor_vehicle.vendor_id = vendor.id

			group by vendor.vendor_name) as trip_count on trip_count.vendor_name = vendor.vendor_name
	where vendor.company_id = {}  
	order by trip_count.trip_id desc)a where a.trip_counts is not null and a.vendor_name is not null {}""".format(companyId,query_string)

		cur.execute(total_records)
		tot = cur.fetchall()

		total_count = pd.DataFrame(tot, columns = ["trip_count","vendor_name"])

		query = """select * from (SELECT distinct trip_count.trip_id as trip_counts,case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null 
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name
	FROM VENDOR
	left join vendor_vehicle on vendor.id = vendor_vehicle.vendor_id
	left join vehicle on vendor_vehicle.vehicle_id = vehicle.id
	left join company on vehicle.company_id = company.id
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join (select count(trip.id) as trip_id, vendor.vendor_name from trip
			join vehicle_booking_details on trip.vehicle_booking_details = vehicle_booking_details.id
			join vehicle on vehicle.id = vehicle_booking_details.vehicle_id
			join vendor_vehicle on vendor_vehicle.vehicle_id = vehicle.id
			join vendor on vendor_vehicle.vendor_id = vendor.id

			group by vendor.vendor_name) as trip_count on trip_count.vendor_name = vendor.vendor_name
	where vendor.company_id = {}
	order by trip_count.trip_id desc)a where a.trip_counts is not null and a.vendor_name is not null""".format(companyId)
		cur.execute(query)
		device_performance_ = cur.fetchall()
		device_performance_ = pd.DataFrame(device_performance_,columns = ['trip_counts',"transporter_name"])

		device_performance_ = device_performance_.replace("NaT", "")

		first_five = device_performance_.head(5)
		last_five = device_performance_.tail(5)

		json_first_five = json.loads(first_five.to_json(orient='records'))
		json_last_five = json.loads(last_five.to_json(orient='records'))

		json_output['top_5']=json_first_five
		json_output['bottom_5']=json_last_five

		if limit and offset:
			query = """select * from (SELECT distinct trip_count.trip_id as trip_counts,case when (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1)) is not null 
		then (select vendor_name from vendor where id=(select vendor_id from vendor_vehicle where vehicle_id=vehicle.id limit 1))
		else company.company_name end as vendor_name,
		 '' as grade, '' as on_time_delivery_compliance, '' as tat_compliance,
			   '' as distance_compliance, '' as safety_compliace, '' as transporter_rating
	FROM VENDOR
	left join vendor_vehicle on vendor.id = vendor_vehicle.vendor_id
	left join vehicle on vendor_vehicle.vehicle_id = vehicle.id
	left join company on vehicle.company_id = company.id
	left join vehicle_booking_details on vehicle_booking_details.vehicle_id = vehicle.id
	left join (select count(trip.id) as trip_id, vendor.vendor_name from trip
			join vehicle_booking_details on trip.vehicle_booking_details = vehicle_booking_details.id
			join vehicle on vehicle.id = vehicle_booking_details.vehicle_id
			join vendor_vehicle on vendor_vehicle.vehicle_id = vehicle.id
			join vendor on vendor_vehicle.vendor_id = vendor.id

			group by vendor.vendor_name) as trip_count on trip_count.vendor_name = vendor.vendor_name
	where vendor.company_id = {}
	order by trip_count.trip_id desc)a where a.trip_counts is not null and a.vendor_name is not null {} limit {} offset {}
			""".format(companyId,query_string,limit,(int(offset)-1)*int(limit))
		cur.execute(query)
		device_performance = cur.fetchall()
		device_performance = pd.DataFrame(device_performance,columns = ["trip_counts","vendor_name","grade","on_time_delivery_compliance","tat_compliance","distance_compliance","safety_compliace","transporter_rating"])


		isDataframeEmpty = True
		if int(offset) >= math.ceil(int(len(total_count.index)) / int(limit)):
			isDataframeEmpty = False

		meta = {
			'totalRecords' : len(device_performance.index),
			'totalPages' :  math.ceil(int(len(total_count.index)) / int(limit)),
			'currentPage' : int(offset),
			'currentPageCount': len(device_performance.index),
			'nextPage' : bool(isDataframeEmpty)
		}

		json_output['meta'] = meta
		json_output['data'] = json.loads(device_performance.to_json(orient='records'))
		
	cur.close()
	conn.close()
	#except:
		#cur1.close()
		#conn1.close()

	content = gzip.compress(json.dumps(json_output).encode('utf8'), 5)
	response = make_response(content)

	response.headers['Content-Encoding'] = 'gzip'
	response.headers['Content-length'] = len(content)

	return response



if __name__ == "__main__":
	app.run(host='demo2.transo.in',threaded = True,port=5001,ssl_context=('/etc/letsencrypt/live/demo2.transo.in/fullchain.pem', '/etc/letsencrypt/live/demo2.transo.in/privkey.pem'))