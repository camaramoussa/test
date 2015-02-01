# -*- coding: utf-8 -*-

'''
Plays back a simulation for anomaly detection.
It's a generic process for computing averages and standard deviation by slot time.

Outputs a well-formatted CSV with ANOMALY or NORMAL label for all data points. 
In other words, all raw data points will be issued in the final output. Data within history will be labeled "NA"
'''
import csv as csv
import numpy as np
import datetime as dt
import pprint as pp
import pandas as pd

pretty_printer = pp.PrettyPrinter(depth=5)
HISTORY_DAYS = 30

'''
Indexes in input file
'''
CUSTOMER_IDX = 0
START_TIME_IDX = 1
END_TIME_IDX = 2
START_QUARTER_IDX = 3
END_QUARTER_IDX = 4
POSITION_IDX = 5
ELAPSED_TIME_SECS_IDX = 6

class SlotTime:
    def __init__(self, start_exclusive, end_inclusive):
        self.start_exclusive = start_exclusive
        self.end_inclusive = end_inclusive
    def is_in_range(self, time):
        """Return true if time is in the range ]start, end]"""
        if self.start_exclusive <= self.end_inclusive:
            return self.start_exclusive < time <= self.end_inclusive
        else:
            return self.start_exclusive < time or time <= self.end_inclusive
    def __str__(self): return "]" + str(self.start_exclusive) + " - " + str(self.end_inclusive) + "]" 
    def __repr__(self): return self.__str__()
    
    def __hash__(self):
        return hash((self.start_exclusive, self.end_inclusive))

    def __eq__(self, other):
        return (self.start_exclusive, self.end_inclusive) == (other.start_exclusive, other.end_inclusive)

class Summary:
    def __init__(self, mean, std, lenght):
        self.mean = mean
        self.std = std
        self.lenght = lenght
    
    def __str__(self): return "Mean: " + str(self.mean) + ", Std: " + str(self.std) + ", Lenght: " + str(self.lenght) 
    def __repr__(self): return self.__str__()

NO_SUMMARY_AVAILABLE = Summary(-1, -1, -1)

'''
Reads a CSV file and put data in a matrix
'''
def csv_to_matrix(csv_file_name, has_header = True):
    csv_file_object = csv.reader(open(csv_file_name, 'rb'))
    if has_header:
        header = csv_file_object.next() # just skip header

    data=[]                          
    for row in csv_file_object:      
        data.append(row)
    data = np.array(data)

    return data;

'''
Creates a slot time-location structure to organize mesures.
---------------        -----------
| 08:00-09:00 | -----> | BEDROOM | ----> [mesure1, mesure2, mesureN]
|     ...     |        | KITCHEN | ----> [mesure1, ..., mesureN]
| 22:30-08:00 |        -----------
---------------
'''
def create_slot_timetable(customer_slot_timetable, customer_locations):
    slots = {}
    for slot in customer_slot_timetable:
        start_hour, start_min = slot[1].split(":")
        end_hour, end_min = slot[2].split(":")
        slot_time_entry =  SlotTime(dt.time(int(start_hour), int(start_min), 0), 
                                    dt.time(int(end_hour), int(end_min), 0))
        locations = {}
        for location in customer_locations:
            locations[location] = []
        slots[slot_time_entry] = locations
    return slots

'''
Computes statistics from lower_date to upper_date range.
'''
def create_summary(behaviour_timetable, lower_date, upper_date, customer_slot_timetable, customer_locations):
    summary_accumulator = create_slot_timetable(customer_slot_timetable, customer_locations)
    daterange = pd.date_range(lower_date, upper_date)
    for single_date in daterange:
        single_day_as_str = single_date.strftime("%Y-%m-%d")
        
        if single_day_as_str in behaviour_timetable:
            for slot_time, locations in behaviour_timetable[single_day_as_str].iteritems():
                for location, data_points  in locations.iteritems():
                    mesures = map(lambda dp: int(dp[ELAPSED_TIME_SECS_IDX]), data_points)
                    #pretty_printer.pprint(mesures)
                    summary_accumulator[slot_time][location].extend(mesures)

   
                   
    for slot_time, locations in summary_accumulator.iteritems():
        for location, mesures  in locations.iteritems():
            if len(mesures) > 0:
                summary_accumulator[slot_time][location] = Summary(np.mean(mesures), np.std(mesures), len(mesures))
            else:
                summary_accumulator[slot_time][location] = None
    return summary_accumulator


'''
Finds the next available day in the customer dataset
'''
def find_next_day(start, max_date, btt):
    next_day = start + dt.timedelta(days=1)
    if next_day > max_date:
        return max_date
    
    if next_day.isoformat() in btt:
        return next_day
    else:
        return find_next_day(next_day, max_date, btt)

def csv_generate_rich_datapoint_first_sampling(csv_output, btt_entry, days):
     for slot_time, locations in btt_entry.iteritems():
        for location, data_points in locations.iteritems():
            for data_point in data_points:
                csv_generate_rich_datapoint(csv_output, data_point, days, slot_time, "NA", NO_SUMMARY_AVAILABLE) 

def csv_generate_rich_datapoint(csv_output, data_point, days, slot_time, status, summary_stats):
    csv_output.writerow([data_point[CUSTOMER_IDX], days, data_point[START_TIME_IDX], data_point[END_TIME_IDX], slot_time, data_point[POSITION_IDX], status, data_point[ELAPSED_TIME_SECS_IDX], "{0:.2f}".format(summary_stats.mean), "{0:.2f}".format(summary_stats.std), summary_stats.lenght])

if __name__ == '__main__':

    start_process = dt.datetime.utcnow()

    #read input sets
    input_file_name = "data-exports/stay-at-room-span-250814-to-231114-filtered.csv"
    #input_file_name = 'test-dataset-for-ads-hist-6-days.csv'
    (filename_part, extension) = input_file_name.split('.')
    dataset = csv_to_matrix(input_file_name)
    slot_timetable_set = csv_to_matrix('dataset-slot-timetable-4.csv')

    #prepare the output CSV
    rich_datapoints_file = open(filename_part + '-rich-datapoints.csv', "wb")
    csv_output = csv.writer(rich_datapoints_file)
    #write the header
    csv_output.writerow(["customer_id", "day_number", "start_time", "end_time", "slot_time", "location", "status", "span_time_secs", "avg_history", "std_history", "lenght_history"])

    #discovery customers and their locations
    customers_list = np.unique(dataset[0::,0])

    locations_table = {}
    for c in customers_list:
        locations_table[c] = np.unique(dataset[ dataset[0::,0] == c, 5])

    #now, compute statistics for each customer
    for customer in customers_list:

        print "Start processing for client %s ..." % customer

        customer_dataset = dataset [ dataset[:,0] == customer ]
        customer_slot_timetable = slot_timetable_set[ slot_timetable_set[0::,0] == customer]
        customer_locations = locations_table[customer]

        # first we have to organize all mesures by date/slot time/location. This is called behaviourTimeTable
        btt = {}
        min_date = dt.date.today()
        max_date = dt.date(1970,1,1)

        print "\tPhase 1: organizing all data points by date/slot time/location..."

        for data_point in customer_dataset:
            datetime_point = data_point[END_TIME_IDX]
            date_as_str, time_as_str = datetime_point.split(" ")
            
            year, month, day = date_as_str.split("-")
            date_point = dt.date(int(year), int(month), int(day))

            min_date = min(min_date, date_point)
            max_date = max(max_date, date_point)
            
            hour, minutes, seconds = time_as_str.split(":")
            time_point = dt.time(int(hour), int(minutes), int(seconds))
            
            if date_as_str not in btt:
                #create slots
                btt[date_as_str] = create_slot_timetable(customer_slot_timetable, customer_locations)
            
            for slot_time, locations in btt[date_as_str].iteritems():
                if slot_time.is_in_range(time_point):
                    location = data_point[POSITION_IDX]
                    locations[location].append(data_point)
                    break

        # and then, find out the history period to compute average and std 
        print "\tPhase 2: Ran through %s to %s. Now, computing history period over %s days..." % (min_date, max_date, HISTORY_DAYS)
        days = 1
        #generate first day
        csv_generate_rich_datapoint_first_sampling(csv_output, btt[min_date.isoformat()], days)

        hist_date_lower = hist_date_upper = min_date
        while days < HISTORY_DAYS and hist_date_upper < max_date:
            hist_date_upper = hist_date_upper + dt.timedelta(days=1)
            if hist_date_upper.isoformat() in btt:
                days = days + 1
                csv_generate_rich_datapoint_first_sampling(csv_output, btt[hist_date_upper.isoformat()], days)

        #pretty_printer.pprint(btt)

        # now, starts simulation from the day after the end of history period
        simulation_range = pd.date_range(hist_date_upper + dt.timedelta(days=1), max_date)
        print "\tPhase 3: simulation anomaly detection from %s to %s" % (hist_date_upper + dt.timedelta(days=1), max_date)

        for single_date in simulation_range:
            single_day_as_str = single_date.strftime("%Y-%m-%d")
            
            if single_day_as_str in btt:
                days = days + 1
                summary = create_summary(btt, hist_date_lower, hist_date_upper, customer_slot_timetable, customer_locations)
                #pretty_printer.pprint(summary)

                day_slot_timetable = btt[single_day_as_str]
                for slot_time, locations in day_slot_timetable.iteritems():
                    for location, data_points in locations.iteritems():
                        for data_point in data_points:
                            summary_per_slot_location = summary[slot_time][location]
                            if summary_per_slot_location is not None:
                                threshold = summary_per_slot_location.mean + summary_per_slot_location.std
                                mesure = int(data_point[ELAPSED_TIME_SECS_IDX])
                                #print "Analyzing mesure %s, threshold %s" % (mesure, threshold)
                                if mesure > threshold:
                                    csv_generate_rich_datapoint(csv_output, data_point, days, slot_time, "ANOMALY", summary_per_slot_location)
                                else:
                                    csv_generate_rich_datapoint(csv_output, data_point, days, slot_time, "NORMAL", summary_per_slot_location)
                            else:
                                csv_generate_rich_datapoint(csv_output, data_point, days, slot_time, "NA", NO_SUMMARY_AVAILABLE)
                #adujst next day
                hist_date_lower = find_next_day(hist_date_lower, max_date, btt)
                hist_date_upper = single_date

    # Close out the output CSV.
    rich_datapoints_file.close()
    
    elapsed_time = dt.datetime.utcnow() - start_process;
    print "It took %s seconds." % elapsed_time.seconds