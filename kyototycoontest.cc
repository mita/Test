#include <string.h>
#include <ktremotedb.h>

extern "C" {
#include "testutil.h"
}

using namespace std;
using namespace kyototycoon;

static void *open_db(struct benchmark_config *config)
{
	RemoteDB *db = new RemoteDB();

	if (!db->open(config->host, config->port)) {
		die("open error: %s", db->error().name());
	}
	return db;
}

static void close_db(void *db)
{
	RemoteDB *rdb = (RemoteDB *)db;

	if (!rdb->close()){
		die("close error: %s", rdb->error().name());
	}
	delete rdb;
}

static bool debug = false;

static void put_test(void *db, int num, int vsiz, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	string value(vsiz, '\0');
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		string key(keygen_next_key(&keygen));

		rdb->set(key, value);
	}
}

static void get_test(void *db, int num, int vsiz, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		string key(keygen_next_key(&keygen));
		string value;

		rdb->get(key, &value);
		if (debug && vsiz != value.size())
			die("Unexpected value size: %d", value.size());
	}
}

static void putlist_bin_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	string value(vsiz, '\0');
	vector<RemoteDB::BulkRecord> bulkrecs;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		string key(keygen_next_key(&keygen));
		RemoteDB::BulkRecord rec = { 0, key, value, kc::INT64MAX };

		bulkrecs.push_back(rec);

		if (bulkrecs.size() >= batch) {
			rdb->set_bulk_binary(bulkrecs);
			bulkrecs.clear();
		}
	}
	if (bulkrecs.size())
		rdb->set_bulk_binary(bulkrecs);
}

static void putlist_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	string value(vsiz, '\0');
	map<string, string> list;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		list[string (keygen_next_key(&keygen))] = value;

		if (list.size() >= batch) {
			rdb->set_bulk(list);
			list.clear();
		}
	}
	if (list.size())
		rdb->set_bulk(list);
}

static void check_keys(vector<string> *list, int num, unsigned int seed)
{
	int i;
	struct keygen keygen;

	if (!debug)
		return;

	keygen_init(&keygen, seed);

	if (num != list->size())
		die("Unexpected key num: %d", list->size());

	for (i = 0; i < num; i++) {
		const char *key = (*list)[i].data();
		int ksiz = (*list)[i].size();

		if (strncmp(keygen_next_key(&keygen), key, ksiz))
			die("Unexpected key");
	}
}

static void fwmkeys_test(void *db, int num, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	char prefix[KEYGEN_PREFIX_SIZE + 1];
	vector<string> list;

	keygen_init(&keygen, seed);
	rdb->match_prefix(string (keygen_prefix(&keygen, prefix)), &list, -1);
	check_keys(&list, num, seed);
}

static void check_bin_records(vector<RemoteDB::BulkRecord> *bulkrecs,
			struct keygen *keygen, int vsiz, int batch)
{
	int recnum;

	if (!debug)
		return;

	recnum = bulkrecs->size();

	if (recnum != batch)
		die("Unexpected list size %d", recnum);

	vector<RemoteDB::BulkRecord>::iterator it = bulkrecs->begin();
	vector<RemoteDB::BulkRecord>::iterator end = bulkrecs->end();

	while (it != end) {
		const char *key = it->key.data();
		int keysiz = it->key.size();
		int valsiz = it->value.size();

		if (strncmp(keygen_next_key(keygen), key, keysiz))
			die("Unexpected key");
		if (valsiz != vsiz)
			die("Unexpected value size %d", valsiz);

		it++;
	}
}


static void check_records(map<string, string> *recs, struct keygen *keygen,
			int vsiz, int batch)
{
	int recnum;

	if (!debug)
		return;

	recnum = recs->size();

	if (recnum != batch)
		die("Unexpected list size %d", recnum);

	map<string, string>::const_iterator it = recs->begin();
	map<string, string>::const_iterator end = recs->end();

	while (it != end) {
		const char *key = it->first.data();
		int keysiz = it->first.size();
		int valsiz = it->second.size();

		if (strncmp(keygen_next_key(keygen), key, keysiz))
			die("Unexpected key");
		if (valsiz != vsiz)
			die("Unexpected value size %d", valsiz);

		it++;
	}
}

static void getlist_bin_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	struct keygen keygen_for_check;
	vector<RemoteDB::BulkRecord> bulkrecs;
	int i;

	keygen_init(&keygen, seed);
	keygen_init(&keygen_for_check, seed);

	for (i = 0; i < num; i++) {
		string key(keygen_next_key(&keygen));
		RemoteDB::BulkRecord rec = { 0, key, "", 0 };

		bulkrecs.push_back(rec);

		if (bulkrecs.size() >= batch) {
			rdb->get_bulk_binary(&bulkrecs);
			check_bin_records(&bulkrecs, &keygen_for_check, vsiz,
					bulkrecs.size());
			bulkrecs.clear();
		}
	}
	if (bulkrecs.size()) {
		rdb->get_bulk_binary(&bulkrecs);
		check_bin_records(&bulkrecs, &keygen_for_check, vsiz,
				bulkrecs.size());
	}
}

static void getlist_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	struct keygen keygen_for_check;
	vector<string> list;
	map<string, string> recs;
	int i;

	keygen_init(&keygen, seed);
	keygen_init(&keygen_for_check, seed);

	for (i = 0; i < num; i++) {
		list.push_back(string(keygen_next_key(&keygen)));

		if (list.size() >= batch) {
			rdb->get_bulk(list, &recs);
			check_records(&recs, &keygen_for_check, vsiz,
					list.size());
			recs.clear();
			list.clear();
		}
	}
	if (list.size()) {
		rdb->get_bulk(list, &recs);
		check_records(&recs, &keygen_for_check, vsiz, list.size());
		recs.clear();
	}
}

static void range_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	char prefix[KEYGEN_PREFIX_SIZE + 1];
	string key, value;
	int nrecs = 0;

	keygen_init(&keygen, seed);
	keygen_prefix(&keygen, prefix);
	RemoteDB::Cursor *cur = rdb->cursor();
	cur->jump(prefix, strlen(prefix));

	while (cur->get(&key, &value)) {
		if (strncmp(key.data(), prefix, strlen(prefix))) {
			break;
		}
		if (debug && vsiz != value.size())
			die("Unexpected value size: %d", value.size());
		if (debug && strncmp(keygen_next_key(&keygen),
				key.data(), key.size()))
			die("Unexpected key");
		nrecs++;
	}
	if (debug && num != nrecs)
		die("Unexpected record num: %d", nrecs);

	delete cur;
}

static void rangeout_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	die("rangeout_test is not implemented");
}

static void outlist_bin_test(void *db, const char *command, int num, int batch,
			unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	vector<RemoteDB::BulkRecord> bulkrecs;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		string key(keygen_next_key(&keygen));
		RemoteDB::BulkRecord rec = { 0, key, "", 0 };

		bulkrecs.push_back(rec);

		if (bulkrecs.size() >= batch) {
			rdb->remove_bulk_binary(bulkrecs);
			bulkrecs.clear();
		}
	}
	if (bulkrecs.size())
		rdb->remove_bulk_binary(bulkrecs);
}

static void outlist_test(void *db, const char *command, int num, int batch,
			unsigned int seed)
{
	RemoteDB *rdb = (RemoteDB *)db;
	struct keygen keygen;
	vector<string> list;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		list.push_back(string(keygen_next_key(&keygen)));

		if (list.size() >= batch) {
			rdb->remove_bulk(list);
			list.clear();
		}
	}
	if (list.size())
		rdb->remove_bulk(list);
}

static struct benchmark_config config;

int main(int argc, char **argv)
{
	config.producer = "nop";
	config.consumer = "nop";
	config.host = "localhost";
	config.port = 1978;
	config.num = 5000000;
	config.vsiz = 100;
	config.batch = 1000;
	config.producer_thnum = 1;
	config.consumer_thnum = 1;
	config.debug = false;
	config.verbose = 1;
	config.ops.open_db = open_db;
	config.ops.close_db = close_db;
	config.ops.put_test = put_test;
	config.ops.get_test = get_test;
	config.ops.fwmkeys_test = fwmkeys_test;
	config.ops.rangeout_test = rangeout_test;
	if (0) { /* HTTP */
		config.ops.putlist_test = putlist_test;
		config.ops.getlist_test = getlist_test;
		config.ops.outlist_test = outlist_test;
	} else { /* binary protocol */
		config.ops.putlist_test = putlist_bin_test;
		config.ops.getlist_test = getlist_bin_test;
		config.ops.outlist_test = outlist_bin_test;
	}
	config.ops.range_test = range_test;
	
	parse_options(&config, argc, argv);
	debug = config.debug;
	benchmark(&config);

	return 0;
}
