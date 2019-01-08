from mysql.connector.pooling import MySQLConnectionPool
import time

class DbCommunicator:
    def __init__(self, db_name, db_user, password, host):
        db_config = {
          "database": db_name,
          "user":     db_user,
          "host":     host,
          "passwd":   password,
          "autocommit": "True"
        }

        # dbconfig = {
        #     "database": "Registry",
        #     "user": "root",
        #     "host": '0.0.0.0',
        #     "passwd": "root",
        #     "autocommit": "True"
        # }

        self.cnxpool = MySQLConnectionPool(pool_name="mypool", pool_size=32, **db_config)

    def get_connection_to_db(self):
        while True:
            try:
                # print("Get connection DB")
                connection = self.cnxpool.get_connection()
                return connection
            except:
                print("Can't get connection DB")
                pass

    def get_sources(self, platform_id=None, source_id=None, source_status=None, source_type=None, metric_status=None, get_metric=True, get_source_id_of_metric=False):
        # start = time.time()
        # start_tong = time.time()
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        have_where = False
        sources = []
        query_source = """SELECT SourceId, EndPoint, SourceStatus, Description, SourceType, Label, PlatformId, LocalId
                            FROM IoTSource"""

        if platform_id is not None:
            if have_where is False:
                query_source = query_source + """ WHERE PlatformId='{}'""".format(platform_id)
                have_where = True
            else:
                query_source = query_source + """ and PlatformId='{}'""".format(platform_id)

        if source_id is not None:
            if have_where is False:
                query_source = query_source + """ WHERE SourceId='{}'""".format(source_id)
                have_where = True
            else:
                query_source = query_source + """ and SourceId='{}'""".format(source_id)

        if source_type is not None:
            if have_where is False:
                query_source = query_source + """ WHERE SourceType='{}'""".format(source_type)
                have_where = True
            else:
                query_source = query_source + """ and SourceType='{}'""".format(source_type)

        if (source_status is not None) and (source_status in ['active', 'inactive']):
            if have_where is False:
                query_source = query_source + """ WHERE SourceStatus='{}'""".format(source_status)
                have_where = True
            else:
                query_source = query_source + """ and SourceStatus='{}'""".format(source_status)
        # print(query_source)


        query_join = """SELECT sources.SourceId, sources.EndPoint, sources.SourceStatus, sources.Description, sources.SourceType, sources.Label, sources.PlatformId, sources.LocalId,
                        Metric.MetricId, Metric.MetricName, Metric.MetricType, Metric.Unit, Metric.MetricDomain, Metric.MetricStatus, Metric.MetricLocalId 
                        FROM ( """+query_source+ """ ) AS sources 
                        INNER JOIN Metric
                        ON sources.SourceId = Metric.SourceId"""

        if (metric_status is not None) and (metric_status in ['active', 'inactive']):
            query_join = query_join + """ WHERE MetricStatus='{}'""".format(metric_status)

        query_join = query_join + """ ORDER BY sources.SourceId ASC"""
        # print(query_join)
        cursor_1.execute(query_join)
        # print("TIME QUERRY thong tin SOURCE API: {}".format(time.time() - start))

        rows_source = cursor_1.fetchall()
        start_index = 0
        # print("LENGH: {}".format(len(rows_source)))
        while start_index < len(rows_source):
            end_index = start_index + 1
            while end_index < len(rows_source) and rows_source[start_index][0] == rows_source[end_index][0]:
                end_index = end_index + 1

            source = {
                'information': {},
                'metrics': []
            }

            row = rows_source[start_index]

            source['information']['SourceId'] = row[0]
            source['information']['EndPoint'] = row[1]

            if source_status is not None:
                source['information']['SourceStatus'] = row[2]

            source['information']['Description'] = row[3]
            source['information']['SourceType'] = row[4]

            if source['information']['SourceType'] == "Thing":
                # start = time.time()
                self.get_thing_info(cursor_1, source['information']['SourceId'], source)
                # print("TIME QUERRY thong tin Thing API: {}".format(time.time() - start))

            source['information']['Label'] = row[5]
            source['information']['PlatformId'] = row[6]
            source['information']['LocalId'] = row[7]

            for i in range(start_index, end_index):
                metric = {}
                row_metric = rows_source[i]
                metric['MetricId'] = row_metric[8]
                metric['MetricName'] = row_metric[9]
                metric['MetricType'] = row_metric[10]
                metric['Unit'] = row_metric[11]
                metric['MetricDomain'] = row_metric[12]
                if metric_status is not None:
                    metric['MetricStatus'] = row_metric[13]
                if get_source_id_of_metric is True:
                    metric['SourceId'] = row_metric[0]

                metric['MetricLocalId'] = row_metric[14]

                source['metrics'].append(metric)
            start_index = end_index
            sources.append(source)

        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        # print("TONG TIME QUERRY SOURCE API: {}".format(time.time()-start_tong))
        return sources

    def get_metrics(self, cursor_1, source_id=None, metric_status=None, get_source_id=False):
        # cnx_1 = self.get_connection_to_db()
        # cursor_1 = cnx_1.cursor()
        have_where = False
        query_metric = """SELECT MetricId, MetricName, MetricType, Unit, MetricDomain, MetricStatus, SourceId, MetricLocalId
                            FROM Metric"""

        if source_id is not None:
            if have_where is False:
                query_metric = query_metric + """ WHERE SourceId='{}'""".format(source_id)
                have_where = True
            else:
                query_metric = query_metric + """ and SourceId='{}'""".format(source_id)

        if (metric_status is not None) and (metric_status in ['active', 'inactive']):
            if have_where is False:
                query_metric = query_metric + """ WHERE MetricStatus='{}'""".format(metric_status)
                have_where = True
            else:
                query_metric = query_metric + """ and MetricStatus='{}'""".format(metric_status)

        cursor_1.execute(query_metric)
        rows_metric = cursor_1.fetchall()
        metrics = []
        for row_metric in rows_metric:
            metric = {}
            metric['MetricId'] = row_metric[0]
            metric['MetricName'] = row_metric[1]
            metric['MetricType'] = row_metric[2]
            metric['Unit'] = row_metric[3]
            metric['MetricDomain'] = row_metric[4]
            if metric_status is not None:
                metric['MetricStatus'] = row_metric[5]
            if get_source_id is True:
                metric['SourceId'] = row_metric[6]

            metric['MetricLocalId'] = row_metric[7]

            metrics.append(metric)
        # cnx_1.commit()
        # cursor_1.close()
        # cnx_1.close()
        return metrics

    def get_thing_info(self, cursor_1, source_id, source_info):
        # cnx_1 = self.get_connection_to_db()
        # cursor_1 = cnx_1.cursor()
        cursor_1.execute("""SELECT ThingName
                            FROM Thing  WHERE ThingGlobalId=%s""", (str(source_id),))
        thing_info = cursor_1.fetchone()
        source_info['information']['ThingName'] = thing_info[0]
        # cnx_1.commit()
        # cursor_1.close()
        # cnx_1.close()

    def update_info_sources(self, infos, new_source=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        records_source = []
        records_thing = []

        if new_source is False:
            for info in infos:
                records_source.append((info['EndPoint'], info['SourceStatus'], info['Description'], str(info['Label']),
                                       info['LocalId'], str(info['SourceId'])))
                if info['SourceType'] == "Thing":
                    records_thing.append((info['ThingName'], str(info['SourceId'])))
            # start = time.time()
            cursor_1.executemany("""UPDATE IoTSource SET EndPoint=%s, SourceStatus=%s, Description=%s, Label=%s, LocalId=%s  WHERE SourceId=%s""", records_source)
            # print("THOI GIAN DB SOURCE: {}".format(time.time()-start))
            if len(records_thing) > 0:

                cursor_1.executemany("""UPDATE Thing SET ThingName=%s  WHERE ThingGlobalId=%s""", records_thing)
            # elif source_type == "Platform":
            #     cursor_1.execute("""UPDATE Platform SET PlatformName=%s, PlatformType=%s WHERE PlatformId=%s""",
            #         (info['PlatformName'], info['PlatformType'], str(source_id)))

        else:
            for info in infos:
                records_source.append((str(info['SourceId']), str(info['EndPoint']), str(info['SourceStatus']), str(info['Description']), str(info['SourceType']), str(info['Label']), str(info['PlatformId']), str(info['LocalId'])))
                if info['SourceType'] == "Thing":
                    records_thing.append((str(info['SourceId']), info['ThingName']))

            cursor_1.executemany("""INSERT INTO IoTSource VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", records_source)

            if len(records_thing) > 0:
                cursor_1.executemany("""INSERT INTO Thing VALUES (%s,%s)""", records_thing)
            # elif source_type == "Platform":
            #     cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s)""",
            #                      (source_id, info['PlatformName'], info['PlatformType']))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def get_platforms(self, platform_id = None, platform_status = None):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        query_statement = """SELECT PlatformId, PlatformName, PlatformType, PlatformHost, PlatformPort, PlatformStatus, LastResponse FROM Platform"""

        have_where = False
        platforms = []

        if platform_id is not None:
            if have_where is False:
                query_statement = query_statement + """ WHERE PlatformId='{}'""".format(platform_id)
                have_where = True
            else:
                query_statement = query_statement + """ and PlatformId='{}'""".format(platform_id)

        if (platform_status is not None) and (platform_status in ['active', 'inactive']):
            if have_where is False:
                query_statement = query_statement + """ WHERE PlatformStatus='{}'""".format(platform_status)
                have_where = True
            else:
                query_statement = query_statement + """ and PlatformId='{}'""".format(platform_status)

        cursor_1.execute(query_statement)
        rows = cursor_1.fetchall()
        for row in rows:
            platform = {
                'PlatformId': row[0],
                'PlatformName': row[1],
                'PlatformType': row[2],
                'PlatformHost': row[3],
                'PlatformPort': row[4],
                'PlatformStatus': row[5],
                'LastResponse': row[6]
            }

            platforms.append(platform)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        return platforms

    def update_platform(self, info_platform, new_platform=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        platform_id = info_platform['PlatformId']
        platform_name = info_platform['PlatformName']
        platform_type = info_platform['PlatformType']
        platform_host = info_platform['PlatformHost']
        platform_port = info_platform['PlatformPort']
        platform_status = info_platform['PlatformStatus']
        last_response = info_platform['LastResponse']

        if new_platform is False:
            cursor_1.execute("""UPDATE Platform SET PlatformName=%s, PlatformType=%s, PlatformHost=%s, PlatformPort=%s, PlatformStatus=%s, LastResponse=%s WHERE PlatformId=%s""",
                             (platform_name, platform_type, platform_host, platform_port, platform_status, last_response, platform_id))
        else:
            cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                             (platform_id, platform_name , platform_type, platform_host, platform_port, platform_status, last_response))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def update_metrics(self, info_metrics, new_metric=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()

        records_metric = []
        if new_metric is False:
            for info_metric in info_metrics:
                records_metric.append((str(info_metric['SourceId']), str(info_metric['MetricName']),
                                       str(info_metric['MetricType']), str(info_metric['Unit']),
                                       str(info_metric['MetricDomain']), str(info_metric['MetricStatus']),
                                       str(info_metric['MetricLocalId']), str(info_metric['MetricId'])))
            cursor_1.executemany("""UPDATE Metric SET SourceId=%s, MetricName=%s, MetricType=%s, Unit=%s, MetricDomain=%s, MetricStatus=%s, MetricLocalId=%s
                                WHERE MetricId=%s""", records_metric)
        else:
            for info_metric in info_metrics:
                records_metric.append((str(info_metric['MetricId']), str(info_metric['SourceId']), str(info_metric['MetricName']),
                                       str(info_metric['MetricType']), str(info_metric['Unit']),
                                       str(info_metric['MetricDomain']), str(info_metric['MetricStatus']),
                                       str(info_metric['MetricLocalId'])))
            cursor_1.executemany("""INSERT INTO Metric VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",records_metric)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()


# DbCommunicator("mysql", "root", "root", "0.0.0.0").test()
