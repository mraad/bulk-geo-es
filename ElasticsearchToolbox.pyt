import traceback

import math
import arcpy
from elasticsearch import Elasticsearch


class Toolbox(object):
    def __init__(self):
        self.label = "Toolbox"
        self.alias = "toolbox"
        self.tools = [BulkTool, GeoDistanceTool]


class BulkTool(object):
    def __init__(self):
        self.label = "Bulk Load Features"
        self.description = "Bulk load features from a feature class into Elasticsearch"
        self.canRunInBackground = True

    def getParameterInfo(self):
        input_fc = arcpy.Parameter(name="orig", displayName="Input FeatureClass", direction="Input",
                                   datatype="Table View",
                                   parameterType="Required")

        es_hosts = arcpy.Parameter(name="es_hosts", displayName="ES Host(s)", direction="Input",
                                   datatype="String",
                                   parameterType="Required")
        es_hosts.value = "192.168.99.100"

        index_type = arcpy.Parameter(name="dest", displayName="ES Index/Mapping", direction="Input",
                                     datatype="String",
                                     parameterType="Required")
        index_type.value = "miami/broadcast"

        num_shards = arcpy.Parameter(name="num_shards", displayName="Num Shards", direction="Input",
                                     datatype="Long",
                                     parameterType="Required")
        num_shards.value = 1

        num_replicas = arcpy.Parameter(name="num_replicas", displayName="Number of Replicas", direction="Input",
                                       datatype="Long",
                                       parameterType="Required")
        num_replicas.value = 0

        batch_size = arcpy.Parameter(name="batch_size", displayName="Batch Size", direction="Input",
                                     datatype="Long",
                                     parameterType="Required")
        batch_size.value = 5000

        shape_precision = arcpy.Parameter(name="shape_precision", displayName="Shape Precision", direction="Input",
                                          datatype="String",
                                          parameterType="Required")
        shape_precision.value = "1km"

        geo_shape = arcpy.Parameter(name="geo_shape", displayName="Index Points as Geo Shapes",
                                    direction="Input",
                                    datatype="Boolean",
                                    parameterType="Optional")

        bulk_refresh = arcpy.Parameter(name="bulk_refresh", displayName="Refresh Index After Bulk Load",
                                       direction="Input",
                                       datatype="Boolean",
                                       parameterType="Optional")

        convert_polygon = arcpy.Parameter(name="convert_polygon", displayName="Convert Polygons to MultiPolygons",
                                          direction="Input",
                                          datatype="Boolean",
                                          parameterType="Optional")

        return [input_fc, es_hosts, index_type, num_shards, num_replicas, batch_size,
                shape_precision, geo_shape, bulk_refresh, convert_polygon]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    '''
    def convertPolygon2MultipolyonStr(self, shape):
        return eval(
            str(shape).replace("Polygon", "MultiPolygon").
                replace('(', '[').
                replace(')', ']').
                replace("[[[", "[[[[").
                replace("]],", "]]],").
                replace(", [[", ", [[[").
                replace("]]]}", "]]]]}"))
    '''

    def convertPolygon(self, shape):
        if shape["type"] == "Polygon":
            shape["coordinates"] = [[coords] for coords in shape["coordinates"]]
            shape["type"] = "MultiPolygon"
        return shape

    def convertNoop(self, shape):
        return shape

    def execute(self, parameters, messages):
        orig = parameters[0].valueAsText

        description = arcpy.Describe(orig)

        geo_shape = parameters[7].value is True

        bulk_refresh = parameters[8].value is True

        convert_polygon = parameters[9].value is True

        geo_point = False if geo_shape else description.shapeType == 'Point'

        shape_name, shape_type = ('SHAPE@XY', 'geo_point') if geo_point else ('SHAPE@', 'geo_shape')

        field_list = [shape_name, 'OID@']
        field_dict = {
            "shape": {
                "type": shape_type,
                "precision": parameters[6].valueAsText
            }
        }
        for field in description.fields:
            if field.name.lower() not in (
                    'shape_length', 'shape_area', 'shape.len', 'shape.length', 'shape_len',
                    'shape.area') and field.name.find(".") == -1 and field.type in (
                    'String', 'Integer', 'Double', 'Float', 'Date'):
                field_list.append(field.name)
                field_dict[field.name] = {"type": field.type.lower()}

        index_name, type_name = parameters[2].valueAsText.split('/')

        es = Elasticsearch(hosts=parameters[1].valueAsText.split(','), timeout=60)

        mapping = {
            type_name: {
                "_all": {
                    "enabled": False
                },
                "_source": {
                    "enabled": True
                },
                "properties": field_dict
            }
        }
        settings_mappings = {
            "settings": {
                "number_of_shards": parameters[3].value,
                "number_of_replicas": parameters[4].value
            },
            "mappings": mapping
        }
        if es.indices.exists(index=index_name):
            es.indices.put_mapping(index=index_name, doc_type=type_name, body=mapping)
        else:
            es.indices.create(index=index_name, body=settings_mappings)

        convert = self.convertNoop if not convert_polygon else self.convertPolygon

        batch_size = parameters[5].value
        batch_count = 0
        body = []
        field_list_len = len(field_list)
        with arcpy.da.SearchCursor(orig, field_list) as cursor:
            for row in cursor:
                body.append({
                    "index": {
                        "_id": row[1]
                    }
                })
                row0 = row[0]
                shape = [row0[0], row0[1]] if geo_point else row0.__geo_interface__
                # arcpy.AddMessage(shape)
                doc = {"shape": convert(shape)}
                for index in range(2, field_list_len):
                    doc[field_list[index]] = row[index]
                body.append(doc)
                batch_count += 1
                if batch_count == batch_size:
                    res = es.bulk(index=index_name, doc_type=type_name, body=body, refresh=bulk_refresh)
                    if res["errors"]:
                        arcpy.AddWarning(res)
                    batch_count = 0
                    body = []
                    # break
        if batch_count > 0:
            res = es.bulk(index=index_name, doc_type=type_name, body=body, refresh=bulk_refresh)
            if res["errors"]:
                arcpy.AddWarning(res)
        es.indices.flush(index=index_name)
        return


class BaseTool(object):
    def __init__(self):
        self.RAD = 6378137.0
        self.RAD2 = self.RAD * 0.5
        self.LON = self.RAD * math.pi / 180.0
        self.D2R = math.pi / 180.0

    def lonToX(self, l):
        return l * self.LON

    def latToY(self, l):
        rad = l * self.D2R
        sin = math.sin(rad)
        return self.RAD2 * math.log((1.0 + sin) / (1.0 - sin))

    def deleteFC(self, fc):
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

    def getParamString(self, name="in", displayName="Label", value=""):
        param = arcpy.Parameter(
            name=name,
            displayName=displayName,
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param.value = value
        return param

    def getParamName(self, displayName="Layer name", value="output"):
        return self.getParamString(name="in_name", displayName=displayName, value=value)

    def getParamFC(self):
        paramFC = arcpy.Parameter(
            name="outputFC",
            displayName="outputFC",
            direction="Output",
            datatype="DEFeatureClass",
            parameterType="Derived")
        # paramFC.symbology = "C:/symbology.lyr"
        return paramFC

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return


class GeoDistanceTool(BaseTool):
    def __init__(self):
        super(GeoDistanceTool, self).__init__()
        self.label = "Geo Distance"
        self.description = "Perform geo spatial radius search from a center point on an ES index/mapping"
        self.canRunInBackground = False
        self.es = Elasticsearch(hosts="192.168.99.100")

    def getParameterInfo(self):
        field_mapping = arcpy.Parameter(displayName="Fields",
                                        name="field_mappings",
                                        datatype="Field Mappings",
                                        parameterType="Required",
                                        direction="Input")

        radius = self.getParamString(name="radius", displayName="Radius", value="1km")
        radius.filter.type = "ValueList"
        radius.filter.list = ["100m", "200m", "500m", "1km", "10km"]

        lon = arcpy.Parameter(displayName="Center lon",
                              name="center_lon",
                              datatype="Double",
                              parameterType="Required",
                              direction="Input")
        lon.value = -80.138

        lat = arcpy.Parameter(displayName="Center lat",
                              name="center_lat",
                              datatype="Double",
                              parameterType="Required",
                              direction="Input")
        lat.value = 25.765

        index_name = arcpy.Parameter(displayName="Index Name",
                                     name="index_name",
                                     datatype="String",
                                     parameterType="Required",
                                     direction="Input")
        index_name.value = "miami"

        type_name = arcpy.Parameter(displayName="Type Name",
                                    name="type_name",
                                    datatype="String",
                                    parameterType="Optional",
                                    direction="Input")

        es_hosts = arcpy.Parameter(displayName="ES Host(s)",
                                   name="es_hosts",
                                   datatype="String",
                                   parameterType="Required",
                                   direction="Input")
        es_hosts.value = "192.168.99.100"

        shape_type = self.getParamString(name="shape_type", displayName="Shape Type", value="POINT")
        shape_type.filter.type = "ValueList"
        shape_type.filter.list = ["POINT", "POLYLINE", "POLYGON"]

        return [self.getParamFC(),
                es_hosts,
                index_name,
                type_name,
                field_mapping,
                radius,
                lon,
                lat,
                shape_type
                ]

    def execute(self, parameters, messages):
        arcpy.env.overwriteOutput = True
        try:
            es_hosts = parameters[1].value
            index_name = parameters[2].value
            type_name = parameters[3].value
            fms = parameters[4].value
            radius = parameters[5].value
            center_lon = parameters[6].value
            center_lat = parameters[7].value
            shape_type = parameters[8].value

            spref = arcpy.SpatialReference(4326)

            # fc = os.path.join(arcpy.env.scratchGDB, name)
            # ws = os.path.dirname(fc)

            name = index_name if len(type_name) == 0 else index_name + "_" + type_name
            ws = "in_memory"
            fc = ws + "/" + name

            source = ['shape']
            fields = ['SHAPE@']
            arcpy.management.CreateFeatureclass(ws, name, shape_type, spatial_reference=spref)
            for field in fms.fields:
                arcpy.management.AddField(fc, field.name, field.type, field.precision, field.scale, field.length)
                source.append(field.name)
                fields.append(field.name)

            search_body = {
                "size": 10000,
                "_source": source,
                "query": {
                    "filtered": {
                        "query": {
                            "match_all": {}
                        },
                        "filter": {
                            "geo_shape": {
                                "shape": {
                                    "shape": {
                                        "type": "circle",
                                        "radius": radius,
                                        "coordinates": [center_lon, center_lat]
                                    }
                                }
                            }
                        }
                    }
                }
            }

            es = Elasticsearch(hosts=es_hosts.split(','), timeout=60)
            with arcpy.da.InsertCursor(fc, fields) as cursor:
                doc = es.search(index=index_name, doc_type=type_name, body=search_body)
                for hit in doc['hits']['hits']:
                    src = hit["_source"]
                    row = [arcpy.AsShape(src["shape"])]
                    for field in fms.fields:
                        if field.name in src:
                            row.append(src[field.name])
                        else:
                            row.append(None)
                    cursor.insertRow(row)
                del doc
            parameters[0].value = fc
        except:
            arcpy.AddMessage(traceback.format_exc())
