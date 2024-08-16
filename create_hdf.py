import sys,os
from run_auto_ndacc import run_auto_ndacc
from datetime import datetime
from datetime import timedelta

initfile = sys.argv[1]
gas_nr = sys.argv[2]
start_year = sys.argv[3]
quality = sys.argv[4]
print (initfile)
rd = run_auto_ndacc(initfile)
rd.set_active(int(gas_nr))
if quality == 'FINAL':
    rd.year = start_year
    rd.setup_sunbatch()
    start_date = '{}0101'.format(rd.year)
    end_date = '{}1231'.format(rd.year)
    rd.modify_sunbatch({'daterange':'{} {}'.format(start_date, end_date)}) 
#    rd.retrieve(start_date=start_date,end_date=end_date)
    rd.error_calc(start_date=start_date,end_date=end_date)
    rd.create_tmph5(start_date=start_date,end_date=end_date)
    hdffile = rd.create_hdf(start_date=start_date,end_date=end_date, quality = quality)
elif quality == 'CAMS27':
    rd.year = datetime.now().year
    rd.setup_sunbatch()
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    rd.first_day =  (datetime.now() - timedelta(days=30))
    rd.last_day =  datetime.now() 
#    rd.retrieve()
    rd.error_calc()
    end_date = datetime.now().strftime('%Y%m%d')
    rd.create_tmph5(end_date=end_date)
    hdffile = rd.create_hdf(end_date = end_date, quality = quality)
#    print(os.path.join((rd.hdf_dir, hdffile.hdfFname)))
