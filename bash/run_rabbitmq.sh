while true;
DEBUG=True
do
  if [[ ${DEBUG}]]
	  then
	     python3 ${XL_IDP_ROOT_RABBITMQ}/scripts/receive.py
	else
	  sleep 3600
	  python3 ${XL_IDP_ROOT_RABBITMQ}/scripts/receive.py
done

