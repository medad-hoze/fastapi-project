



from   arcgis.gis import GIS
import pandas as pd
from   fastapi import FastAPI
import uvicorn

app       = FastAPI()

class layers_manager():
    
    def __init__(self,org,agoluser,agolpwd,proj_path):

        self.org        = org
        self.agoluser   = agoluser
        self.agolpwd    = agolpwd
        self.proj_path  = proj_path

        self.get_connection()
        self.tables               = self.portal_item.tables
        self.layers               = self.portal_item.layers
        self.my_layers            = {layer.properties['name']:layer for layer in self.layers}
        self.all_layers           = None
        self.extract_data()
        
            
    def get(self,layer_name):
        return self.my_layers[layer_name]

    def get_connection(self):
        self.gis          = None
        self.gis          = GIS(url=self.org, username=self.agoluser, password=self.agolpwd,timeout=5000)
        self.portal_item  = self.gis.content.get(self.proj_path)

    def extract_data(self):
        
        self.get_connection()
        self.all_layers = []
        for lyr in self.layers:
            name      = lyr.properties['name']
            geom_type = lyr.properties['geometryType']
            fields    = [[f['name'],f['type']] for f in lyr.properties.fields]
            features  = len(lyr.query('1=1'))
            self.all_layers.append({'layer'      :lyr       ,
                                    'name'       :name      ,
                                   'geom_type'   :geom_type ,
                                   'fields'      :[i[0] for i in fields],
                                    'field_type' :[i[1] for i in fields],
                                    'total feat' :features  ,
                                   'type_'       :'layer'    })
            
        for tbl in self.tables:
            name      = tbl.properties['name']
            fields    = [[f['name'],f['type']] for f in tbl.properties.fields]
            features  = len(lyr.query('1=1'))
            self.all_layers.append({'layer'     : tbl,
                                    'name'      : name,
                                    'fields'    : [i[0] for i in fields],
                                    'field_type' :[i[1] for i in fields],
                                    'total feat':features,
                                    'type_'     :'table',
                                    'geom_type' :'nan'   })
            
    def __str__(self):
        print ('#####   layers  #####')
        for lyr in self.layers:
            name      = lyr.properties['name']
            geom_type = lyr.properties['geometryType']
            feat      = len(lyr.query('1=1'))
            fields    = [f['name'] for f in lyr.properties.fields]
            print (f"layer name: {name}, have {feat} features, geometry: {geom_type}, with {len(fields)} fields")
        
        print('####   tables  ####')
        for table in self.tables:
            name      = table.properties['name']
            fields    = [f['name'] for f in table.properties.fields]
            feat      = len(table.query('1=1'))
            print (f"layer name: {name}, have {feat} features, with {len(fields)} fields")

            
    def get_layer(self,check_):
        for obj in self.all_layers:
            if check_ == obj['name']:
                return obj['layer']      
        return 'No layer found'
    
    def extract_rows_from_layer(self,check_):
        
        for obj in self.all_layers:
            if not obj['name'] == check_:continue
            feature_set = obj['layer'].query()

            # Define a helper function to convert a feature to a row dictionary
            def feature_to_row(feature):
                attributes = feature.attributes
                geometry = feature.geometry
                row = {key: attributes[key] for key in attributes if key != 'geometry'}
                if obj['geom_type'] != 'nan':
                    row['geometry'] = geometry
                else:
                    row['geometry'] = 'nan'
                return row

            # Convert the features to rows
            rows = [feature_to_row(feature) for feature in feature_set]

            return rows
        

    def delete_all_features(self,lyer_name):
        feature_layer = self.get_layer(lyer_name)

        object_ids = feature_layer.query(return_ids_only=True)["objectIds"]

        if object_ids:
            result = feature_layer.edit_features(deletes=','.join(map(str, object_ids)))
            return result
        else:
            return "No features to delete."
        
    @ staticmethod
    def split_list(input_list, num_parts):
        if num_parts <= 0:
            raise ValueError("Number of parts must be greater than 0")
        
        part_length, remainder = divmod(len(input_list), num_parts)
        result = []
        current_index = 0
        
        for i in range(num_parts):
            current_part_length = part_length + (1 if i < remainder else 0)
            current_part = input_list[current_index:current_index + current_part_length]
            result.append(current_part)
            current_index += current_part_length

        return result

    def add_rows_to_layer(self,layer_name, rows):

        def create_feature_from_row(row):
            attributes = {key: row[key] for key in row if key != 'geometry'}
            geometry   = row['geometry']
            return {"attributes": attributes, "geometry": geometry}
        
        feature_layer = self.get_layer(layer_name)
        features    = [create_feature_from_row(row) for row in rows]
        print (f'adding {len(features)} features')

        list_features = self.split_list(features, 10)
        print (f'list cut to: {len(list_features)} parts, each  part have: {len(list_features[0])} features')
        
        for part_features in list_features:
            feature_layer.edit_features(adds=part_features)
            self.get_connection()


@app.get('/')
def read_root():
    return {"Hello": "World"}


@app.get('/data_to_agol')
def data_to_agol():


    org          = 'https://kkl.maps.arcgis.com/home'
    agoluser     = 'medadhozekkl'
    agolpwd      = 'medadhozekkl123'

    proj_path    = '7bcf659726df4c8588808c7999cbd35a'  ######  misssing name of the project  ######
    class_proj   = layers_manager(org,agoluser,agolpwd,proj_path)
    data         = class_proj.extract_rows_from_layer('GrazingAllocation')
    return {'data':data}



# data = data_to_agol(org,agoluser,agolpwd)


# uvicorn main:app --reload
# cd C:\Users\medadh\Desktop\medad\Work\REACT\api
# 15308