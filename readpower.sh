export PYTHONPATH=/usr/local/sw/hpclib
alias  python=/usr/local/sw/anaconda/anaconda3/bin/python
cd /home/installer/clusterdata
now=`date +%y%m%d`
python readpower.py --pivot --tare --output "/scratch/installer/power.$now"
