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
		string *value;

		value = rdb->get(key);
		if (debug && vsiz != value->size())
			die("Unexpected value size: %d", value->size());

		delete value;
	}
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
	pair<string, string> *rec;
	int nrecs = 0;

	keygen_init(&keygen, seed);
	keygen_prefix(&keygen, prefix);
	RemoteDB::Cursor *cur = rdb->cursor();
	cur->jump(prefix, strlen(prefix));

	while ((rec = cur->get_pair(NULL, true)) != NULL) {
		if (strncmp(rec->first.data(), prefix, strlen(prefix))) {
			delete rec;
			break;
		}
		if (debug && vsiz != rec->second.size())
			die("Unexpected value size: %d", rec->second.size());
		if (debug && strncmp(keygen_next_key(&keygen),
				rec->first.data(), rec->first.size()))
			die("Unexpected key");
		nrecs++;
		delete rec;
	}
	if (num != nrecs)
		die("Unexpected record num: %d", nrecs);

	delete cur;
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
	config.thnum = 1;
	config.debug = false;
	config.share = 0;
	config.ops.open_db = open_db;
	config.ops.close_db = close_db;
	config.ops.put_test = put_test;
	config.ops.get_test = get_test;
	config.ops.putlist_test = putlist_test;
	config.ops.fwmkeys_test = fwmkeys_test;
	config.ops.getlist_test = getlist_test;
	config.ops.range_test = range_test;
	config.ops.outlist_test = outlist_test;
	
	parse_options(&config, argc, argv);
	debug = config.debug;
	benchmark(&config);

	return 0;
}
