#include <bits/stdc++.h>
#include <pthread.h>
using namespace std;

map<string, vector<pair<int, int> > > mvcc;
map<string, bool> locked;
pthread_mutex_t mutex_lock;

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
	while (op >> txn) {
		if (txn != "BEGIN") break;
		int id;
		op >> id;
		clock_t txn_time = clock();
		result << id << ", BEGIN, " << txn_time << ',' << endl;
		temp_write.clear();
		lock_var.clear();
		while (1) {
			string op_name;
			string var;
			op >> op_name;
			if (op_name == "READ") {
				op >> var;
				if (temp_write.find(var) != temp_write.end()) {
					int val = temp_write[var];
					result << id << ", " << var << ", " << clock() << ", " << val << endl;
				} else {
					pthread_mutex_lock(&mutex_lock);
					int val = mvcc_read(var, txn_time);
					pthread_mutex_unlock(&mutex_lock);
					result << id << ", " << var << ", " << clock() << ", " << val << endl;
				}
			} else if (op_name == "SET") {
				op >> var;
				var.pop_back();
				string var2, arith;
				int val1, val2;
				op >> var2 >> arith >> val2;
				if (temp_write.find(var2) != temp_write.end()) {
					val1 = temp_write[var2];
				} else {
					pthread_mutex_lock(&mutex_lock);
					val1 = mvcc_read(var2, txn_time);
					pthread_mutex_unlock(&mutex_lock);
				}
				if (arith == "+") val1 += val2; else val1 -= val2;
				pthread_mutex_lock(&mutex_lock);
				while (locked[var]);
				locked[var] = true;
				temp_write[var] = val1;
				mvcc_write(var, val1, clock());
				lock_var.insert(var);
				result << id << ", " << var << ", " << clock() << ", " << val1 << endl;
				pthread_mutex_unlock(&mutex_lock);
			} else if (op_name == "COMMIT") {
				op >> id;
				pthread_mutex_lock(&mutex_lock);
				for (auto it = lock_var.begin(); it != lock_var.end(); it ++)
					locked[(*it)] = false;
				pthread_mutex_unlock(&mutex_lock);
				result << id << ", END, " << clock() << ',' << endl;
				break;
			}
		}
	}
	
	op.close();
	result.close();
	return nullptr;
}

int main(int argc, char **argv) {
	if (argc != 2) {
		cout << "Failed to get thread number." << endl;
		return 0;
	}
	int thread_num = atoi(argv[1]);
	pthread_mutex_init(&mutex_lock, nullptr);
	
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
	return 0;
}
