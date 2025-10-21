from src.models import merge_base as merge, predict_base as predict
from src.utils import common as utils 
from pyspark.sql import functions as F, types as T

# define model code, client name
model_code = "cs_v2_202505"
client_name = "TNEX"
model_pkl_name = "final_model"
feature_pkl_name = "feature_dict" 

# define custom code (if need)
outlier = utils.load_pickle(f"models/model_code={model_code}/client={client_name}/outlier_mapping.pkl")

def fill_category(dataset, outlier, cat_feature, **kwargs):
    for col_name, repl in outlier['categorical'].items():
        if col_name in cat_feature:
            dataset = dataset.fillna({col_name: repl})
    return dataset

def fill_numerical(dataset, num_features, **kwargs): 
    def fill_na_outlier(df, col_name, qt99):
        df = df.fillna({col_name : 0})
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) > qt99, F.lit(qt99)).otherwise(F.col(col_name))
        )
        return df
    
    for col_name in num_features:
        if col_name in dataset.columns:
            qt99 = outlier['numerical'][col_name]
            dataset= fill_na_outlier(dataset, col_name, qt99)
    return dataset

handlers = {
    "fill_category" : fill_category,
    "fill_numerical" : fill_numerical
}

external_params = {"outlier" : outlier}

# run merge
merge.run_merge(model_code, client_name)

# run predict
predict.run_predict(model_code, model_pkl_name, client_name, feature_pkl_name, res_num = None, handlers = handlers, external_params = external_params)