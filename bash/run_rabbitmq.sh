while true;
do
	python3 ${XL_IDP_ROOT_RABBITMQ}/scripts/receive.py
	sleep 3600;
done