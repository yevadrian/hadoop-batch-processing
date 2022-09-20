#!/usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = ['order_id', 'order_date', 'user_id', 'payment_name', 'shipper_name', 'order_price', 'order_discount', 'voucher_name', 'voucher_price', 'order_total', 'rating_status']

def csv_readline(line):
    for row in csv.reader([line]):
        return row

class OrderDateSum(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.sorter)]

    def mapper(self, _, line):
        row = dict(zip(cols, csv_readline(line)))
        if row['order_id'] != 'order_id':
            yield row['order_date'][:7], int(row['order_total'])

    def reducer(self, key, values):
        yield None, (key, sum(values))

    def sorter(self, key, values):
        data = []
        for order_date, order_total in values:
            data.append((order_date, order_total))
            data.sort()
        for order_date, order_total in data:
            yield order_date, order_total

if __name__ == '__main__':
    OrderDateSum.run()