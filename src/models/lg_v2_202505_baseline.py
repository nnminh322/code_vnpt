from src.models import merge_base as merge, predict_base as predict

# define model code, client name
model_code = "lg_v2_202505"
client_name = "baseline"
model_pkl_name = "lg_model_202505"
feature_pkl_name = "feature_dict" 

# define custom code (if need)
def fill_na_cat(df, **kwargs):
    fillna_strategy = {
        'location_top1_province_l4w' : 'other', 
        'prepaid_danhba_age_group_l1w' : 'ukn'
    }
    df = df.fillna(fillna_strategy)
    return df

handlers = {"fill_na_cat" : fill_na_cat}

# run merge
merge.run_merge(model_code, client_name)

# run predict
predict.run_predict(model_code, model_pkl_name, client_name, feature_pkl_name, res_num = None, handlers = handlers)
