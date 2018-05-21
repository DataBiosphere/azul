   #!/bin/bash

   export ES_SERVICE="<YOUR_ES_ENDPOINT_HERE>"
   export ES_PORT="443"
   export ES_PROTOCOL="https"
   python make_fake_data.py fake_data_template.json 40000 250
