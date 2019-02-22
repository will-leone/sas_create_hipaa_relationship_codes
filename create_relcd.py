"""
Create SAS Formats for Individual Relationship Codes

Last Updated: February 22, 2019

Purpose:
  - Retrieve the 2004 CMS crosswalk for Individual Relationship Codes.
  - Clean and export this data to SAS format tables on the SAS server.
  - Create a CSV copy of the crosswalk to accompany these datasets.

 Prerequisites:
  - In addition to installing Python/Anaconda on your computer,
    you will also need to install the tabula and saspy modules using the
    'pip install tabula' and 'conda install saspy' commands in Anaconda
    Prompt.
  - You will also need to configure saspy

Instructions: Copy-paste the following into Anaconda Prompt.

    python
    from os import chdir
    chdir("//grid/sasprod/dw/formats/source/code")
    import create_relcd

"""

import saspy
import pandas as pd
import tabula
import time

# OUTPUT DATA PARAMETERS
sas = saspy.SASsession(cfgname='pdw_config')
sas_code = sas.submit("""
    LIBNAME fmt "/sasprod/dw/formats/source/staging";
    """)
grid = ("//grid/sasprod/dw/formats/source")
out_file = ("//grid/sasprod/dw/formats/source/references/"
            "cms_relcd.xlsx")

# Pull CMS.GOV PDF data into an in-memory Pandas DataFrame
pdf_site = ('https://www.cms.gov/Regulations-and-Guidance/Guidance/'
            'Transmittals/downloads/R9MSP.pdf')
outdf = tabula.read_pdf(pdf_site, pages=[7, 8], lattice=True)
outdf.columns = ([
    "HIPAA Individual Relationship Code"
    , "CWF Patient Relationship Code"
    , "Code Description"
    ])
outdf.to_excel(out_file, sheet_name='relcd', engine='xlsxwriter')
outdf.drop("CWF Patient Relationship Code", axis=1, inplace=True)
outdf.columns = (["start", "label"])
outdf.insert(loc=2, column='fmtname', value='relcd')
outdf.insert(loc=3, column='type', value='C')
del_list = list()

# Format the HIPAA IR codes (relcd format's start values)
for index in range(len(outdf)):
    outdf.iat[index, 0] = str(outdf.iat[index, 0]).strip()
    if len(outdf.iat[index, 0]) == 1:    # add back leading 0's
        outdf.iat[index, 0] = '0' + str(outdf.iat[index, 0])
    elif len(outdf.iat[index, 0]) > 6:    # these are not code records
        del_list.append(outdf.iat[index, 0])
    if '?' in outdf.iat[index, 1]:
        outdf.iat[index, 1] = 'Other'
    elif '\r' in outdf.iat[index, 1]:
        outdf.iat[index, 1] = outdf.iat[index, 1].replace('\r', ' ')
for code in del_list:
    cindex = outdf.index[outdf.start == code].tolist()[0]
    outdf.drop(cindex, inplace=True)
dblindex = outdf.index[outdf.start == '32,33'].tolist()[0]
outdf.iat[dblindex, 0] = '32'
outdf.append(pd.DataFrame(
    data=pd.Series(
        data=[
            '33'
            , list(outdf.loc[outdf.start == '32'].iloc[0])[1]
            , 'relcd'
            , 'C'
        ]
    ))
    , ignore_index=True)
outdf.sort_index(inplace=True)
outdf.reset_index(drop=True, inplace=True)

# Export the finalized DataFrames
try:
    sas_out = sas.df2sd(outdf, table='relcd', libref='fmt')
finally:
    while 'TABLE_EXISTS= 1' not in sas.saslog():
        time.sleep(1)
    print(sas.saslog())
    sas.disconnect()
