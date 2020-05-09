#include <bits/stdc++.h>
#include <pthread.h>
using namespace std;

map<string, vector<pair<int, int> > > mvcc;
map<string, int> locked;
pthread_mutex_t mutex_lock;
pthread_barrier_t barrier;

class operation {
public:
	int type;
	string var1, var2, arith;
	int val;
};

int mvcc_read(string var, int txn_time) {
	//cout << var << endl;
	auto element = mvcc.find(var);
	if (element == mvcc.end()) return -1;
	for (auto it = (*element).second.rbegin(); it != (*element).second.rend(); it ++) {
		//cout << (*it).second << endl;
		if ((*it).second <= txn_time) return (*it).first;
	}
	return -1;
}

void mvcc_write(string var, int val, int txn_time) {
	auto element = mvcc.find(var);
	(*element).second.push_back(make_pair(val, txn_time));
}

void *thread_work(void *rank) {
	int my_rank = *((int*)rank);
	//cout << my_rank << endl;
	my_rank ++;
	ifstream op;
	op.open("thread_" + to_string(my_rank) + ".txt", ios::in);
	if (!op) return nullptr;
	ofstream result;
	result.open("output_thread_" + to_string(my_rank) + ".csv", ios::out);
	if (!result) return nullptr;
	result << "transaction_id, type, time, value" << endl;
	
	map<string, int> temp_write;
	set<string> lock_var;
	string txn;
	pthread_barrier_wait(&barrier);
	vector<operation> ops;
	while (op >> txn) {
		if (txn != "BEGIN") break;
		int id;
		op >> id;
		clock_t txn_time = clock();
		result << id << ", BEGIN, " << txn_time << ',' << endl;
		temp_write.clear();
		lock_var.clear();
		string output = "";
		ops.clear();
		while (1) {
			string op_name, var;
			op >> op_name;
			if (op_name == "READ") {
				op >> var;
				ops.push_back((operation){0, var, "", "", 0});
			} else if (op_name == "SET") {
				op >> var;
				var.pop_back();
				string var2, arith;
				int val;
				op >> var2 >> arith >> val;
				ops.push_back((operation){1, var, var2, arith, val});
			} else if (op_name == "COMMIT") {
				op >> id;
				break;
			}
		}
		int cnt = 0;
		while (cnt < ops.size()) {
			if (ops[cnt].type == 0) {
				string var = ops[cnt].var1;
				if (temp_write.find(var) != temp_write.end()) {
					int val = temp_write[var];
					output = output + to_string(id) + ", " + var + ", " + to_string(clock()) + ", " + to_string(val) + "\n";
				} else {
					pthread_mutex_lock(&mutex_lock);
					int val = mvcc_read(var, txn_time);
					pthread_mutex_unlock(&mutex_lock);
					output = output + to_string(id) + ", " + var + ", " + to_string(clock()) + ", " + to_string(val) + "\n";
				}
			} else {
				string var1 = ops[cnt].var1, var2 = ops[cnt].var2, arith = ops[cnt].arith;
				int val1, val2 = ops[cnt].val;
				if (temp_write.find(var2) != temp_write.end()) {
					val1 = temp_write[var2];
				} else {
					pthread_mutex_lock(&mutex_lock);
					val1 = mvcc_read(var2, txn_time);
					pthread_mutex_unlock(&mutex_lock);
				}
				if (arith == "+") val1 += val2; else val1 -= val2;
				clock_t start = clock();
				bool deadlock = false;
				while (1) {
					pthread_mutex_lock(&mutex_lock);
					int belong = locked[var1];
					pthread_mutex_unlock(&mutex_lock);
					if (belong == 0 || belong == my_rank)
						break;
					clock_t now = clock();
					if ((belong < my_rank) && ((double)(now - start) / CLOCKS_PER_SEC > .001)) {
						//printf("%d\n", id);
						deadlock = true;
						break;
					}
				}
				if (deadlock) {
					pthread_mutex_lock(&mutex_lock);
					for (auto it = lock_var.begin(); it != lock_var.end(); it ++)
						locked[(*it)] = 0;
					pthread_mutex_unlock(&mutex_lock);
					clock_t start = clock();
					while ((double)(clock() - start) / CLOCKS_PER_SEC < .001);
					lock_var.clear();
					temp_write.clear();
					output = "";
					cnt = 0;
					continue;
				}
				pthread_mutex_lock(&mutex_lock);
				locked[var1] = my_rank;
				pthread_mutex_unlock(&mutex_lock);
				temp_write[var1] = val1;
				mvcc_write(var1, val1, clock());
				lock_var.insert(var1);
				output = output + to_string(id) + ", " + var1 + ", " + to_string(clock()) + ", " + to_string(val1) + "\n";
			}
			cnt ++;
		}
		if (!lock_var.empty()) {
			pthread_mutex_lock(&mutex_lock);
			for (auto it = lock_var.begin(); it != lock_var.end(); it ++)
				locked[(*it)] = 0;
			pthread_mutex_unlock(&mutex_lock);
		}
		result << output;
		result << id << ", END, " << clock() << ',' << endl;
	}
	
	op.close();
	result.close();
	return nullptr;
}

int main(int argc, char **argv) {
	srand(time(0));
	if (argc != 2) {
		cout << "Failed to get thread number." << endl;
		return 0;
	}
	int thread_num = atoi(argv[1]);
	pthread_mutex_init(&mutex_lock, nullptr);
	pthread_barrier_init(&barrier, nullptr, thread_num);
	
	ifstream prepare;
	prepare.open("data_prepare.txt", ios::in);
	if (!prepare) {
		cout << "Failed to prepare data." << endl;
		return 0;
	}
	
	string op;
	string var;
	int val;
	while (prepare >> op >> var >> val) {
		//cout << var << " " << val << endl;
		mvcc[var] = vector<pair<int, int> >(1, make_pair(val, 0));
	}
	prepare.close();
	
	pthread_t *thread_handles = new pthread_t[thread_num];
	for (int i = 0; i < thread_num; i ++) {
		int *id = new int(i);
		pthread_create(&thread_handles[i], nullptr, thread_work, id);
	}
	for (int i = 0; i < thread_num; i ++)
		pthread_join(thread_handles[i], nullptr);
	
	pthread_mutex_destroy(&mutex_lock);
	pthread_barrier_destroy(&barrier);
	return 0;
}
