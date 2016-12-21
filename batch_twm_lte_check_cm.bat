rem use: for /l %i in (20160803,1,20160817) do batch_twm_lte_check_cm.bat %i
set /a d1=%1-1
etl check cm %1 %d1%
