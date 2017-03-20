echo "processing start!"
##python Parallel_JParser.py /mnt/f/Demoonism/DataScientist/Spark/data/batch1 /mnt/f/Demoonism/DataScientist/Spark/out/out_A &
##python Parallel_JParser.py /mnt/a/Spark/batch2 /mnt/f/Demoonism/DataScientist/Spark/out/out_B &

#python Parallel_JParser.py /mnt/1e69d2b1-91a9-473c-a164-db90daf43a3d/Backup_tw_2013_2_8/2013-02/2013-02-01 ../output/out

python Parallel_JParser_v4.py /mnt/1e69d2b1-91a9-473c-a164-db90daf43a3d/Backup_tw_2013_2_6/ &
python Parallel_JParser_v4.py /mnt/2b53fde0-61da-4eeb-a038-9910540ff9ad/Backup_tw_2013_7_12/ &
python Parallel_JParser_v4.py /mnt/381c2633-4d72-4555-9be8-19e922cce4a1/Backup_tw_2014_1_6/ &
python Parallel_JParser_v4.py /mnt/4e8ba653-f2f0-4e18-a51e-458026833dee/Backup_tw_2014_7_12/ &
