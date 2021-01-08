from pymongo import MongoClient
import datetime as dt
import random
from pymongo.collection import Collection


def create_partsupp_collection(mydb):
    mycol = mydb["partsupp"]
    mycol.create_index("r_name")
    insert_into_partsupp(mycol)


def create_order_collection(mydb):
    mycol = mydb["order"]
    mycol.create_index("c_mktsegment")
    mycol.create_index("l_shipdate")
    mycol.create_index("r_name")
    insert_into_order(mycol)


# DATA
key = [1, 2, 3, 4, 5]
flag = ['A', 'B', 'C', 'A', 'B']
brand = ["Nike", "Adidas", "New Balance", "Oakley", "Burton"]
address = ["Barcelona", "Mollet", "Corçà", "Delft", "Chicago"]
nation = ["Spain", "Spain", "Spain", "Netherlands", "United States"]
region = ["Catalunya", "Catalunya", "Catalunya", "Delft", "Illinois"]
price = [10, 20, 30, 40, 50]


def insert_into_partsupp(partSuppCol):
    partSuppCol.drop()
    for i in [0, 1, 2, 3, 4]:
        doc = {
            "_id": "{}_{}".format(key[i], key[i]),
            "ps_part": {
                "p_partkey": key[i],
                "p_name": "Partkey" + str(key[i]),
                "p_mfgr": "ABCDEFG",
                "p_brand": brand[i],
                "p_type": "Running",
                "p_size": random.randint(40, 45),
                "p_container": "Container 2" + str(key[i]),
                "p_retailprice": float(random.randint(1000, 5000) / 100),
                "p_comment": "Everything OK"
            },
            "ps_supplier": {
                "s_suppkey": key[i],
                "s_name": "Suppkey" + str(key[i]),
                "s_address": address[i],
                "s_nation": {
                    "n_nationkey": key[i],
                    "n_name": nation[i],
                    "n_region": {
                        "r_regionkey": key[i],
                        "r_name": region[i],
                        "r_comment": "Ok"
                    },
                    "n_comment": "Ok"
                },
                "s_phone": random.randint(600000000, 699999999),
                "s_acctbal": random.random(),
                "s_comment": "Ok"
            },
            "ps_availqty": random.randint(100, 500),
            "ps_supplycost": float(random.randint(100, 500) / 100),
            "ps_comment": "Everything correct"
        }
        partSuppCol.insert_one(doc)


def insert_into_order(orderCol):
    orderCol.drop()
    name = ["Pau", "Laia", "Joan", "Jordi", "Josep"]
    month = [12, 11, 10, 9, 8]
    lineNumber = [21, 34, 54, 21, 65]
    for i in [0, 1, 2, 3, 4]:
        doc = {
            "_id": "{}".format(key[i]),
            "o_orderkey": key[i],
            "o_customer": {
                "c_custkey": key[i],
                "c_name": name[i],
                "c_address": address[i],
                "c_nation": {
                    "n_nationkey": key[i],
                    "n_name": nation[i],
                    "n_region": {
                        "r_regionkey": key[i],
                        "r_name": region[i],
                    },
                },
                "c_phone": random.randint(600000000, 699999999),
                "c_acctbal": random.random(),
                "c_mktsegment": str(random.randint(100000, 999999)),
                "c_comment": "Ok",
            },
            "o_lineitems": [{
                "_id": "{}_{}".format(key[i], lineNumber[i]),
                "l_orderkey": key[i],
                "l_partkey": key[i],
                "l_suppkey": key[i],
                "l_linenumber": lineNumber[i],
                "l_quantity": random.randint(1, 5),
                "l_extendedprice": price[i],
                "l_discount": random.random(),
                "l_tax": random.random(),
                "l_returnflag": flag[i],
                "l_linestatus": flag[i],
                "l_shipdate": dt.datetime(2020, month[i], 16),
                "l_commitdate": dt.datetime(2020, month[i], 16),
                "l_receiptdate": dt.datetime(2020, month[i], 16),
                "l_shipinstruct": "Shipping normal",
                "l_shipmode": flag[i],
                "l_comment": "No comment"
            }],
            "o_orderstatus": "A",
            "o_totalprice": price[i],
            "o_orderdate": dt.datetime(2020, 12, 16),
            "o_orderpriority": "MMM",
            "o_clerk": "AAA",
            "o_shippriority": random.randint(1, 3),
            "o_comment": "Ok"
        }
        orderCol.insert_one(doc)


def query1(orderCol, date):
    return orderCol.aggregate(
        [
            {"$match":
                 {"o_lineitems.l_shipdate": {"$lte": date}}
             },
            {"$project":
                 {"_id": "$_id",
                  "l_returnflag": {"$first": "$o_lineitems.l_returnflag"},
                  "l_linestatus": {"$first": "$o_lineitems.l_linestatus"},
                  "l_quantity": {"$first": "$o_lineitems.l_quantity"},
                  "l_extendedprice": {"$first": "$o_lineitems.l_extendedprice"},
                  "l_discount": {"$first": "$o_lineitems.l_discount"},
                  "l_tax": {"$first": "$o_lineitems.l_tax"}
                  }
             },
            {"$group":
                 {"_id": {"l_returnflag": "$l_returnflag",
                          "l_linestatus": "$l_linestatus"},
                  "l_returnflag": {"$first": "$l_returnflag"},
                  "l_linestatus": {"$first": "$l_linestatus"},
                  "sum_qty": {"$sum": "$l_quantity"},  # sum(l_quantity)
                  "sum_base_price": {"$sum": "$l_extendedprice"},  # sum(l_extendedprice)
                  "sum_disc_price": {"$sum":
                                         {"$multiply":
                                              ["$l_extendedprice",
                                               {"$subtract": [1, "$l_discount"]}]}}, # sum(l_extendedprice*(1-l_discount))
                  "sum_charge": {"$sum": {
                      "$multiply": [{"$multiply":
                                         ["$l_extendedprice",
                                          {"$subtract": [1, "$l_discount"]}]},
                                    {"$add": [1, "$l_tax"]}]}},  # sum(l_extendedprice*(1-l_discount)*(1+l_tax))

                  "avg_qty": {"$avg": "$l_quantity"},  # avg(l_quantity)
                  "avg_price": {"$avg": "$l_extendedprice"},  # avg(l_extendedprice)
                  "avg_disc": {"$avg": "$l_discount"},  # avg(l_discount)
                  "count_order": {"$sum": 1},  # count(*)
                  }
             },
            {"$sort":
                 {"l_returnflag": 1,
                  "l_linestatus": 1}
             }
        ])


def query2(partSuppCol: Collection, size, type, region):
    result_subquery = query2_2(partSuppCol, region)
    supplycost = 1.0
    for row in result_subquery:
        supplycost = row["min_supplycost"]
        break

    return partSuppCol.aggregate(
        [
            {"$match":
                {"$and": [
                    {"ps_part.p_size": {"$eq": size}},
                    {"ps_part.p_type": {"$regex": type}},
                    {"ps_supplier.s_nation.n_region.r_name": {"$eq": region}},
                    {"ps_supplycost": {"$eq": supplycost}}
                ]}
            },
            {"$project":
                 {"_id": "$_id",
                  "s_acctbal": "$ps_supplier.s_acctbal",
                  "s_name": "$ps_supplier.s_name",
                  "n_name": "$ps_supplier.s_nation.n_name",
                  "p_partkey": "$ps_part.p_partkey",
                  "p_mfgr": "$ps_part.p_mfgr",
                  "s_address": "$ps_supplier.s_address",
                  "s_phone": "$ps_supplier.s_phone",
                  "s_comment": "$ps_supplier.s_comment",
                  }
             },
            {"$sort":
                 {"s_acctbal": -1,
                  "n_name": 1,
                  "s_name": 1,
                  "p_partkey": 1}
             }
        ])


def query2_2(partSuppCol, region):
    return partSuppCol.aggregate(
        [
            {"$match":
                 {"ps_supplier.s_nation.n_region.r_name": {"$eq": region}}
             },
            {"$project": {
                "min_supplycost": {"$min": "$ps_supplycost"}
            }},
            {"$sort":
                 {"min_supplycost": 1}
             }
        ])


def query3(orderCol: Collection, segment, date1, date2):
    return orderCol.aggregate(
        [
            {"$match":
                {"$and": [
                    {"o_orderdate": {"$lt": date1},
                     "o_lineitems.l_shipdate": {"$gt": date2},
                     "o_customer.c_mktsegment": {"$eq": segment}}
                ]}
            },
            {"$project":
                 {"_id": "$_id",
                  "l_orderkey": "$o_orderkey",
                  "o_orderdate": "$o_orderdate",
                  "o_shippriority": "$o_shippriority",
                  "l_extendedprice": {"$first": "$o_lineitems.l_extendedprice"},
                  "l_discount": {"$first": "$o_lineitems.l_discount"}
                  }
             },
            {"$group":
                 {"_id": {"l_orderkey": "$l_orderkey",
                          "o_orderdate": "$o_orderdate",
                          "o_shippriority": "$o_shippriority"},
                  "l_orderkey": {"$first": "$l_orderkey"},
                  "revenue": {"$sum":
                                  {"$multiply":
                                       ["$l_extendedprice",
                                        {"$subtract": [1, "$l_discount"]}]}},  # sum(l_extendedprice*(1-l_discount))
                  "o_orderdate": {"$first": "$o_orderdate"},
                  "o_shippriority": {"$first": "$o_shippriority"}
                  }
             },
            {"$sort":
                 {"revenue": -1,
                  "o_orderdate": 1}
             }
        ])


def query4(orderCol, date, region):
    newDate = date.replace(date.year + 1)

    return orderCol.aggregate(
        [
            {"$match":
                {"$and": [
                    {"o_customer.c_nation.n_region.r_name": {"$eq": region}},
                    {"o_orderdate": {"$gte": date}},
                    {"o_orderdate": {"$lt": newDate}}
                ]}
            },
            {"$project":
                 {"_id": "$_id",
                  "n_name": "$o_customer.c_nation.n_name",
                  "l_extendedprice": {"$first": "$o_lineitems.l_extendedprice"},
                  "l_discount": {"$first": "$o_lineitems.l_discount"}
                  }
             },
            {"$group":
                 {"_id": {"n_name": "$n_name"},
                  "n_name": {"$first": "$n_name"},
                  "revenue": {"$sum":
                                  {"$multiply":
                                       ["$l_extendedprice",
                                        {"$subtract": [1, "$l_discount"]}]}},  # sum(l_extendedprice*(1-l_discount))
                  }
             },
            {"$sort":
                 {"revenue": -1}
             }
        ])


def valid_date(date):
    year, month, day = date.split('-')
    try:
        dt.datetime(int(year), int(month), int(day))
    except ValueError:
        return False
    return True


def main():
    client = MongoClient(
        host=['localhost:27017'],
        document_class=dict,
        tz_aware=False,
        connect=True
    )

    mydb = client["paulaia"]

    create_partsupp_collection(mydb)
    create_order_collection(mydb)

    print("What action do you want to do?\n",
          "[1] Execute query 1\n",
          "[2] Execute query 2\n",
          "[3] Execute query 3\n",
          "[4] Execute query 4\n",
          "[0] See the collection's data\n",
          "[-1] Exit")

    op = input("Enter the corresponding number. ")
    op = int(op)

    while op != -1:
        if op == 0:
            print("Collecion Order")
            for doc in mydb["order"].find():
                print(doc)

            print("\nCollection Partsupp")
            for doc in mydb["partsupp"].find():
                print(doc)

        elif op == 1:
            date = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")
            while not valid_date(date):
                date = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")

            q1 = query1(mydb["order"],
                        dt.datetime.strptime(date, "%Y-%m-%d"))

            print("Query 1 results:")
            for row in q1:
                print(row)

        elif op == 2:
            size = input("Enter a part size: ")
            while not size.isdigit():
                size = input("Enter a part size: ")

            type = input("Enter a part type: ")

            region = input("Enter a region name: ")

            q2 = query2(mydb["partsupp"],
                        int(size),
                        str(type),
                        str(region))

            print("Query 2 results:")
            for row in q2:
                print(row)

        elif op == 3:
            mkt_segment = input("Enter a customer mkt_segment: ")

            date1 = input("Enter an order orderdate in the format YYYY-mm-dd: ")
            while not valid_date(date1):
                date1 = input("Enter an order orderdate in the format YYYY-mm-dd: ")

            date2 = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")
            while not valid_date(date2):
                date2 = input("Enter a lineitem shipdate in the format YYYY-mm-dd: ")

            q3 = query3(mydb["order"],
                        str(mkt_segment),
                        dt.datetime.strptime(date1, "%Y-%m-%d"),
                        dt.datetime.strptime(date2, "%Y-%m-%d"))

            print("Query 3 results:")
            for row in q3:
                print(row)

        elif op == 4:
            date = input("Enter an order orderdate in the format YYYY-mm-dd: ")
            while not valid_date(date):
                date = input("Enter an order orderdate in the format YYYY-mm-dd: ")

            region = input("Enter a region name: ")

            q4 = query4(mydb["order"],
                        dt.datetime.strptime(date, "%Y-%m-%d"),
                        region)

            print("Query 4 results:")
            for row in q4:
                print(row)

        elif op == -1:
            break

        print("\nWhat action do you want to do?\n",
              "[1] Execute query 1\n",
              "[2] Execute query 2\n",
              "[3] Execute query 3\n",
              "[4] Execute query 4\n",
              "[0] See the collection's data\n",
              "[-1] Exit")

        op = input("Enter the corresponding number. ")
        op = int(op)


if __name__ == "__main__":
    main()
