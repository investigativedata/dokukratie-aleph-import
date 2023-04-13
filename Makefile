collections := bb be bw by hb he hh mv ni nw rp sh sn st th de_vsberichte sehrgutachten

all: $(collections)

$(collections): %: %.pull %.run %.push

%.run:
	ARCHIVE_BUCKET_PATH=$* MMMETA=./data/$(INSTANCE)/$* python import.py $*

%.pull:
	aws --endpoint-url $(ARCHIVE_ENDPOINT_URL) s3 sync s3://$(ARCHIVE_BUCKET)/$*/_mmmeta/db/ ./data/$(INSTANCE)/$*/_mmmeta/db
	aws --endpoint-url $(ARCHIVE_ENDPOINT_URL) s3 sync s3://$(STATE_BUCKET)/dokukratie/$(INSTANCE)/$*/ ./data/$(INSTANCE)/$*/_mmmeta

%.push:
	aws --endpoint-url $(ARCHIVE_ENDPOINT_URL) s3 cp ./data/$(INSTANCE)/$*/_mmmeta/state.db s3://$(STATE_BUCKET)/dokukratie/$(INSTANCE)/$*/state.db

