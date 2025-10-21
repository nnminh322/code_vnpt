from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import pandas as pd 
import pandas as pd
import numpy as np
import time
import sys
import os
from config_edit import config
import utils

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

device_str = """
    CASE 
        WHEN GSMA_Device_Type IN ('Mobile Phone/Feature phone', 'Smartphone') THEN 'phones'
        WHEN GSMA_Device_Type IN ('Tablet', 'Portable(include PDA)', 'Handheld', 'e-Book', 'Wearable') THEN 'tablets & portables'
        WHEN GSMA_Device_Type IN ('Connected Computer', 'Device for the Automatic Processing of Data (APD)', 'Modem', 'Dongle', 'Module') THEN 'devices telecom-related'
        WHEN GSMA_Device_Type IN ('Vehicle') THEN 'vehicles'
        WHEN GSMA_Device_Type IN ('IoT Device') THEN 'iot devices'
        WHEN GSMA_Device_Type IN ('WLAN Router') THEN 'network devices'
        ELSE 'other'
    END
"""

manu_str = """
    CASE
        WHEN Manufacturer IN ('Samsung', 'Apple', 'Nokia', 'Motorola', 'Huawei', 'Xiaomi', 'Oppo', 'Vivo', 'LG', 'Realme', 'Sony Ericsson', 'Sony', 'HTC', 'Lenovo', 'Google', 'OnePlus') THEN 'global brands'
        WHEN Manufacturer IN ('Sagem', 'Ericsson', 'Alcatel', 'Siemens', 'Philips', 'Sony', 'Archos', 'Wiko') THEN 'european brands'
        WHEN Manufacturer IN ('Micromax', 'Lava', 'Intex', 'Karbonn', 'Spice', 'Celkon', 'iBall', 'Zen', 'Advan') THEN 'indian brands'
        ELSE 'other brands'
    END
"""

top10_manu_str = """
    CASE
        WHEN GSMA_manufacturer IN (
            'Samsung Korea', 'Nokia Corporation', 'Apple Inc', 'HUAWEI Technologies Co Ltd', 
            'Xiaomi Communications Co Ltd', 'Motorola Mobility LLC, a Lenovo Company',
            'Guangdong Oppo Mobile Telecommunications Corp Ltd', 'Vivo Mobile Communication Co Ltd',
            'Realme Chongqing Mobile Telecommunications Corp Ltd', 'LG Electronics Inc.'
        ) THEN GSMA_manufacturer
        ELSE 'other'
    END
"""

lpwan_str = """
    CASE
        WHEN GSMA_LPWAN in ('Not Known', 'Not Supported') THEN GSMA_LPWAN
        WHEN (GSMA_LPWAN IS NULL OR GSMA_LPWAN = '') THEN 'Not Known'
        ELSE 'Supported'
    END
"""

has_nanosim = """
    CASE
         WHEN SIM_Size like "%Nano%" THEN 1
         ELSE 0
    END
"""

has_minisim = """
    CASE
         WHEN SIM_Size like "%Mini%" THEN 1
         ELSE 0
    END
"""

has_microsim = """
    CASE
         WHEN SIM_Size like "%Micro%" THEN 1
         ELSE 0
    END
"""

internal_storage_str = """
    CASE 
         WHEN Internal_Storage_Capacity in (2048, 24576) THEN Internal_Storage_Capacity/1024
         ELSE Internal_Storage_Capacity
    END
"""

bluetooth_str = """
    CASE 
         WHEN GSMA_Bluetooth in ("Y", "Yes", "YES", "yes") then 1
         WHEN GSMA_Bluetooth in ("N", "No", "NO", "no") then 0
         ELSE ""
    END
"""

wlan = """
    CASE 
         WHEN GSMA_WLAN in ("Y", "Yes", "YES", "yes") then 1
         WHEN GSMA_WLAN in ("N", "No", "NO", "no") then 0
         ELSE ""
    END
"""

os_name_str = """
    CASE 
        WHEN OS_Name = 'Android' THEN 'Android'
        WHEN OS_Name = 'iOS' THEN 'iOS'
        ELSE 'others'
    END
"""

chipset_str = """
    CASE 
        WHEN Chipset_Name LIKE 'Snapdragon%' THEN 'Snapdragon'
        WHEN Chipset_Name LIKE 'Exynos%' THEN 'Exynos'
        WHEN Chipset_Name LIKE 'Helio%' THEN 'Helio'
        WHEN Chipset_Name LIKE 'MT%' THEN 'MT'
        WHEN (Chipset_Name rlike '^A[0-9].*')
            AND (
                Chipset_Name LIKE '%Bionic%'
                OR Chipset_Name LIKE '%Fusion%'
                OR Chipset_Name LIKE '%Pro%'
                OR Chipset_Name LIKE '%X%'
            )
            THEN 'Apple'
        ELSE 'others'
    END
"""

gpu_name_str = """
    CASE 
        WHEN GPU_Name LIKE 'Adreno%' THEN 'Adreno'
        WHEN GPU_Name LIKE 'Mali%' THEN 'Mali'
        WHEN GPU_Name LIKE 'PowerVR%' THEN 'PowerVR'
        WHEN (Chipset_Name rlike '^A[0-9].*')
            AND (
                Chipset_Name LIKE '%Bionic%'
                OR Chipset_Name LIKE '%Fusion%'
                OR Chipset_Name LIKE '%Pro%'
                OR Chipset_Name LIKE '%X%'
            )
            THEN 'Apple'
        ELSE 'others'
    END
"""

country_code_str = """
    CASE 
        WHEN GSMA_Country_Code IN (601, 621, 608, 651, 610, 650, 617, 624, 625, 626, 630, 
                                   627, 608, 636, 602, 629, 635, 650, 603, 628, 634, 620, 
                                   628, 631, 639, 650, 625, 603, 640, 635, 608, 616, 617, 
                                   604, 637, 650, 615, 620, 634, 630, 609, 650, 629, 637, 
                                   655, 628, 605, 608, 605, 642, 640, 650) THEN 'Africa'
        WHEN GSMA_Country_Code IN (454, 455, 466, 428, 283, 400, 420, 470, 402, 457, 456, 
                                   460, 286, 282, 404, 510, 432, 418, 425, 440, 421, 401, 
                                   419, 438, 457, 419, 502, 472, 441, 414, 438, 431, 419, 
                                   410, 425, 515, 428, 250, 420, 511, 450, 470, 417, 436, 
                                   520, 457, 286, 437, 424, 436, 452, 427) THEN 'Asia'
        WHEN GSMA_Country_Code IN (276, 213, 283, 232, 257, 206, 293, 286, 219, 286, 230, 
                                   238, 248, 283, 244, 208, 282, 262, 202, 216, 274, 272, 
                                   222, 401, 278, 247, 275, 246, 270, 278, 259, 214, 297, 
                                   204, 295, 242, 260, 268, 226, 250, 274, 220, 231, 293, 
                                   214, 240, 228, 286, 255, 213, 276, 220, 222, 227, 274, 
                                   214, 407, 222, 234) THEN 'Europe'
        WHEN GSMA_Country_Code IN (342, 364, 350, 352, 302, 712, 368, 348, 370, 348, 706, 
                                   372, 706, 338, 334, 341, 710, 714, 340, 344, 345, 374, 
                                   310, 311, 312, 313, 314, 315, 316) THEN 'North America'
        WHEN GSMA_Country_Code IN (722, 736, 724, 730, 732, 740, 738, 744, 716, 744, 748, 
                                   734) THEN 'South America'
        WHEN GSMA_Country_Code IN (505, 520, 559, 535, 550, 543, 530, 544, 598, 546, 597, 
                                   537, 551) THEN 'Ocenia'
        ELSE 'unknown'
    END
"""

mobile_internet_str = """
    CASE
         WHEN T_2G + T_3G + T_4G + T_5G > 1 THEN 1
         ELSE 0
    END
"""

support_html_str = """
    CASE 
         WHEN HTML_Audio + HTML_Canvas + HTML_Inline_SVG + HTML_SVG + HTML_Video > 1 THEN 1
         ELSE 0
    END
"""

support_css_str = """
    CASE
         WHEN CSS_Animations + CSS_Columns + CSS_Transforms + CSS_Transitions > 1 THEN 1
         ELSE 0
    END
"""

support_js_str = """
    CASE
         WHEN JS_Application_Cache + JS_Geo_Location + JS_Indexeddb + JS_Local_Storage + JS_Session_Storage + 
              JS_Web_GL + JS_Web_Sockets + JS_Web_SQL_Database + JS_Web_Workers + JS_Device_Orientation + 
              JS_Device_Motion + JS_Touch_Events + JS_Query_Selector > 1 THEN 1
         ELSE 0
    END
"""

support_img_str = """
    CASE
         WHEN Image_gif87 + Image_gif89a + Image_jpg + Image_png > 1 THEN 1
         ELSE 0
    END

"""
playable_music_str = """
    CASE
         WHEN WMV + MIDI_Monophonic + MIDI_Polyphonic + AMR + MP3 + AAC > 1 THEN 1
         ELSE 0
    END

"""

def gen_device_fts(spark, raw_table_name, snapshot_str):
    
    print("generating fts for:", snapshot_str)
    _start_date = datetime.strptime(snapshot_str, "%Y%m%d") - relativedelta(months=2)
    _start_date = _start_date.strftime("%Y%m%d")

    query_1 = f'''
        select max(s3_file_date) max_date
        from {raw_table_name} where s3_file_date >= '{_start_date}' and s3_file_date <= '{snapshot_str}'
    '''
    
    max_date = utils.spark_read_data_from_singlestore(spark, query_1).toPandas()['max_date'].values[0]
    device = utils.spark_read_data_from_singlestore(spark, f"""
        select * from {raw_table_name} 
        where s3_file_date = '{max_date}'
    """)

    # create fts
    device = device.withColumn("device_age", F.expr(f"{int(snapshot_str[:4])} - Year_Released"))\
                   .withColumn("device_type", F.expr(device_str))\
                   .withColumn("device_manu", F.expr(manu_str))\
                   .withColumn("device_top_manu", F.expr(top10_manu_str))\
                   .withColumn("device_lpwan", F.expr(lpwan_str))\
                   .withColumnRenamed("LTE", "device_has_lte")\
                   .withColumnRenamed("LTE_Advanced", "device_has_lte_advanced")\
                   .withColumnRenamed("Chipset_Maximum_Uplink_Speed", "device_chipset_max_up_speed")\
                   .withColumnRenamed("Chipset_Maximum_Downlink_Speed", "device_chipset_down_up_speed")\
                   .withColumnRenamed("VoLTE", "device_has_volte")\
                   .withColumnRenamed("Wi_Fi", "device_has_wifi")\
                   .withColumnRenamed("VoWiFi", "device_has_vowifi")\
                   .withColumnRenamed("rcs", "device_has_rcs")\
                   .withColumnRenamed("Voice_over_Cellular", "device_has_voice_over_cell")\
                   .withColumn("device_has_mobile_internet", F.expr(mobile_internet_str))\
                   .withColumnRenamed("NB_IoT", "device_has_nb_iot")\
                   .withColumnRenamed("LTE_M", "device_has_lte_m")\
                   .withColumnRenamed("SIM_Slots", "device_sim_slots")\
                   .withColumnRenamed("eSIM_Count", "device_esim_count")\
                   .withColumn("device_has_nanosim", F.expr(has_nanosim))\
                   .withColumn("device_has_minisim", F.expr(has_minisim))\
                   .withColumn("device_has_microsim", F.expr(has_microsim))\
                   .withColumn("device_internal_storage", F.expr(internal_storage_str))\
                   .withColumnRenamed("Expandable_Storage", "device_expandable_storage")\
                   .withColumnRenamed("Touch_Screen", "device_has_touch_screen")\
                   .withColumnRenamed("Diagonal_Screen_Size", "device_screen_size")\
                   .withColumnRenamed("Display_PPI", "device_display_ppi")\
                   .withColumnRenamed("Device_Pixel_Ratio", "device_pixel_ratio")\
                   .withColumnRenamed("Screen_Color_Depth", "device_screen_color_depth")\
                   .withColumnRenamed("CPU_Cores", "device_cpu_cores")\
                   .withColumnRenamed("CPU_Maximum_Frequency", "device_cpu_max_freq")\
                   .withColumnRenamed("32_Bit_Architecture", "device_32_bit")\
                   .withColumnRenamed("64_Bit_Architecture", "device_64_bit")\
                   .withColumn("device_has_bluetooth", F.expr(bluetooth_str))\
                   .withColumn("device_has_wlan", F.expr(wlan))\
                   .withColumnRenamed("GPS_Hardware_Support", "device_has_gps")\
                   .withColumn("device_os_family", F.expr(os_name_str))\
                   .withColumn("device_chipset_family", F.expr(chipset_str))\
                   .withColumn("device_gpu_family", F.expr(gpu_name_str))\
                   .withColumn("device_continent", F.expr(country_code_str))\
                   .withColumnRenamed("Browser_Vendor", "device_browser_vendor")\
                   .withColumnRenamed("Browser_Rendering_Engine", "device_browser_render_engine")\
                   .withColumn("device_support_html", F.expr(support_html_str))\
                   .withColumn("device_support_css", F.expr(support_css_str))\
                   .withColumn("device_support_js", F.expr(support_js_str))\
                   .withColumn("device_support_img", F.expr(support_img_str))\
                   .withColumnRenamed("H_264_Support_In_OS", "device_support_h264")\
                   .withColumnRenamed("H_265_Support_In_OS", "device_support_h265")\
                   .withColumnRenamed("VP9_Support_In_OS", "device_support_vp9")\
                   .withColumn("device_playable_music", F.expr(playable_music_str))

    #Count the number of '/' characters in the column to determine how many splits
    device = device.withColumn("slash_count", F.length(device["Total_RAM"]) - F.length(F.regexp_replace(device["Total_RAM"], "/", "")))
    max_slash = device.selectExpr("MAX(slash_count)").collect()[0][0]
    # Split the "chipset_speeds" column by "/"
    split_col = F.split(device["Total_RAM"], "/")

    # Create columns dynamically based on the slash count
    for i in range(max_slash):
        ram_cate_str = f"""
            CASE
                WHEN ram_{i+1} < 2 THEN '0. <2GB'
                WHEN ram_{i+1} >= 2 AND ram_{i+1} <= 4 THEN '1. 2GB - 4GB'
                WHEN ram_{i+1} > 4 AND ram_{i+1} <= 6 THEN '2. 4GB - 6GB'
                WHEN ram_{i+1} > 8 AND ram_{i+1} <= 12 THEN '3. 8GB - 12GB'
                WHEN ram_{i+1} > 12 THEN '4. 12GB+'
                ELSE '-1. Unknown'
            END
        """
        device = device.withColumn(f"ram_{i+1}", split_col.getItem(i))\
                       .withColumn(f"ram_{i+1}", F.expr(F"CASE WHEN ram_{i+1} > 100 THEN ram_{i+1}/1024 ELSE ram_{i+1} END"))\
                       .withColumn(f"ram_{i+1}", F.expr(ram_cate_str))                         

    device = device.withColumn("device_ram_category", F.greatest(*[F.col(f"ram_{i+1}") for i in range(max_slash)]))\
                   .withColumnRenamed("GSMA_Marketing_Name", "device_marketing_name")\
                   .withColumnRenamed("Standardised_Full_Name", "device_full_name")

    # select columns fts
    device = device.select(
        'TAC', 'device_marketing_name', 'device_full_name',
        'device_age', 'device_type', 'device_manu', 'device_top_manu', 'device_lpwan', 'device_has_lte',
        'device_has_lte_advanced', 'device_chipset_max_up_speed', 'device_chipset_down_up_speed',
        'device_has_volte', 'device_has_wifi', 'device_has_vowifi', 'device_has_rcs', 'device_has_voice_over_cell',
        'device_has_mobile_internet', 'device_has_nb_iot', 'device_has_lte_m', 'device_sim_slots',
        'device_esim_count', 'device_has_nanosim', 'device_has_minisim', 'device_has_microsim',
        'device_internal_storage', 'device_expandable_storage', 'device_has_touch_screen',
        'device_screen_size', 'device_display_ppi', 'device_pixel_ratio',
        'device_screen_color_depth', 'device_cpu_cores', 'device_cpu_max_freq',
        'device_32_bit', 'device_64_bit', 'device_has_bluetooth',
        'device_has_wlan', 'device_has_gps', 'device_os_family',
        'device_chipset_family', 'device_gpu_family', 'device_continent',
        'device_browser_vendor', 'device_browser_render_engine', 'device_support_html',
        'device_support_css', 'device_support_js', 'device_support_img',
        'device_support_h264', 'device_support_h265', 'device_support_vp9', 'device_playable_music',
        'device_ram_category'
    )
    
    ### drop dupplicated TAC 
    device = device.drop_duplicates(subset = ['TAC'])

    target_file_name = f"/date={snapshot_str}"
    utils.save_to_s3(device, 'device' , target_file_name, run_mode)


def process_table(spark, table_name ,ALL_MONTHS, fix_date):
            
    if run_mode == "prod": 
        snapshot_str = ALL_MONTHS[0] + fix_date[0]

        ### run 
        gen_device_fts(spark, table_name, snapshot_str)

    else:
        for MONTH in ALL_MONTHS:
            for _fix_date in fix_date:
                snapshot_str = MONTH + _fix_date
                ### run 
                gen_device_fts(spark, table_name, snapshot_str)

###############################################
#### GET argument  #### addd
table_name = 'blueinfo_tac_gsma'
table_name_on_production = config.table_dict[table_name] ### add

ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
run_mode = config.run_mode

# create Spark session
spark = utils.create_spark_instance(run_mode)
process_table(spark, table_name_on_production ,ALL_MONTHS, fix_dates)




