from mysql.connector.pooling import MySQLConnectionPool


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

    def get_resources(self, platform_id=None, resource_id=None, resource_status=None, resource_type=None, metric_status=None, get_metric=True, get_resource_id_of_metric=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        have_where = False
        resources = []
        query_resource = """SELECT ResourceId, EndPoint, ResourceStatus, Description, ResourceType, Label, PlatformId, LocalId
                            FROM IoTResource"""

        if platform_id is not None:
            if have_where is False:
                query_resource = query_resource + """ WHERE PlatformId='{}'""".format(platform_id)
                have_where = True
            else:
                query_resource = query_resource + """ and PlatformId='{}'""".format(platform_id)

        if resource_id is not None:
            if have_where is False:
                query_resource = query_resource + """ WHERE ResourceId='{}'""".format(resource_id)
                have_where = True
            else:
                query_resource = query_resource + """ and ResourceId='{}'""".format(resource_id)

        if resource_type is not None:
            if have_where is False:
                query_resource = query_resource + """ WHERE ResourceType='{}'""".format(resource_type)
                have_where = True
            else:
                query_resource = query_resource + """ and ResourceType='{}'""".format(resource_type)

        if (resource_status is not None) and (resource_status in ['active', 'inactive']):
            if have_where is False:
                query_resource = query_resource + """ WHERE ResourceStatus='{}'""".format(resource_status)
                have_where = True
            else:
                query_resource = query_resource + """ and ResourceStatus='{}'""".format(resource_status)
        print(query_resource)
        cursor_1.execute(query_resource)
        rows_resource = cursor_1.fetchall()
        for row_resource in rows_resource:
            resource = {
                'information': {},
            }

            resource['information']['ResourceId'] = row_resource[0]
            resource['information']['EndPoint'] = row_resource[1]

            if resource_status is not None:
                resource['information']['ResourceStatus'] = row_resource[2]

            resource['information']['Description'] = row_resource[3]
            resource['information']['ResourceType'] = row_resource[4]
            resource['information']['Label'] = row_resource[5]
            resource['information']['PlatformId'] = row_resource[6]
            resource['information']['LocalId'] = row_resource[7]
            if resource['information']['ResourceType'] == "Thing":
                self.get_thing_info(resource['information']['ResourceId'], resource)

            # elif resource['information']['ResourceType'] == "Platform":
            #     self.get_platform_info(resource['information']['ResourceId'], resource)

            if get_metric is True:
                resource['metrics'] = self.get_metrics(resource_id=row_resource[0], metric_status=metric_status, get_resource_id=get_resource_id_of_metric)

            resources.append(resource)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        return resources

    def get_metrics(self, resource_id=None, metric_status=None, get_resource_id=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        have_where = False
        query_metric = """SELECT MetricId, MetricName, MetricType, Unit, MetricDomain, MetricStatus, ResourceId, MetricLocalId
                            FROM Metric"""

        if resource_id is not None:
            if have_where is False:
                query_metric = query_metric + """ WHERE ResourceId='{}'""".format(resource_id)
                have_where = True
            else:
                query_metric = query_metric + """ and ResourceId='{}'""".format(resource_id)

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
            if get_resource_id is True:
                metric['ResourceId'] = row_metric[6]

            metric['MetricLocalId'] = row_metric[7]

            metrics.append(metric)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        return metrics

    def get_thing_info(self, resource_id, resource_info):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        cursor_1.execute("""SELECT ThingName
                            FROM Thing  WHERE ThingGlobalId=%s""", (str(resource_id),))
        thing_info = cursor_1.fetchone()
        resource_info['information']['ThingName'] = thing_info[0]
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def update_info_resource(self, info, new_resource=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()

        resource_id = info['ResourceId']
        endpoint = info['EndPoint']
        resource_status = info['ResourceStatus']
        description = info['Description']
        resource_type = info['ResourceType']
        label = info['Label']
        platform_id = info['PlatformId']
        local_id = info['LocalId']

        if new_resource is False:

            cursor_1.execute("""UPDATE IoTResource SET EndPoint=%s, ResourceStatus=%s, Description=%s, Label=%s, LocalId=%s  WHERE ResourceId=%s""",
                             (endpoint, resource_status, description, str(label), local_id, str(resource_id)))

            if resource_type == "Thing":
                cursor_1.execute("""UPDATE Thing SET ThingName=%s  WHERE ThingGlobalId=%s""",
                    (info['ThingName'], str(resource_id)))
            # elif resource_type == "Platform":
            #     cursor_1.execute("""UPDATE Platform SET PlatformName=%s, PlatformType=%s WHERE PlatformId=%s""",
            #         (info['PlatformName'], info['PlatformType'], str(resource_id)))

        else:
            cursor_1.execute("""INSERT INTO IoTResource VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                             (resource_id, endpoint, resource_status, description, resource_type, str(label), str(platform_id), local_id))

            if resource_type == "Thing":
                cursor_1.execute("""INSERT INTO Thing VALUES (%s,%s)""",
                                 (resource_id, info['ThingName']))
            # elif resource_type == "Platform":
            #     cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s)""",
            #                      (resource_id, info['PlatformName'], info['PlatformType']))
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

    def update_metric(self, info_metric, new_metric=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        metric_id = info_metric['MetricId']
        resource_id = info_metric['ResourceId']
        metric_name = info_metric['MetricName']
        metric_type = info_metric['MetricType']
        unit = info_metric['Unit']
        metric_domain = info_metric['MetricDomain']
        metric_status = info_metric['MetricStatus']
        metric_local_id = info_metric['MetricLocalId']
        if new_metric is False:
            cursor_1.execute("""UPDATE Metric SET ResourceId=%s, MetricName=%s, MetricType=%s, Unit=%s, MetricDomain=%s, MetricStatus=%s, MetricLocalId=%s
                                WHERE MetricId=%s""", (resource_id, metric_name, metric_type, unit, metric_domain, metric_status, metric_local_id, metric_id))
        else:
            cursor_1.execute("""INSERT INTO Metric VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                             (metric_id, resource_id, metric_name, metric_type, unit, metric_domain, metric_status, metric_local_id))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()


# DbCommunicator("mysql", "root", "root", "0.0.0.0").test()
