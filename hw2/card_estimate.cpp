#include <bits/stdc++.h>

#define integer true
#define text false

#define threshold 100000
#define rate 0.01

using namespace std;

class Column {
public:
	bool type;
	vector<int> sampling_int;
	vector<string> sampling_str;
	unordered_map<int, unordered_set<int> > int_pos;
	unordered_map<string, unordered_set<int> > str_pos;
	Column() {}
};

class Table {
public:
	string tableName;
	int size, sample_size;
	unordered_map<string, Column*> cols;
	Table() { size = sample_size = 0; }
};
unordered_map<string, Table*> tables;
int tableNum;

class Condition {
public:
	string table1, table2;
	string col1, col2;
	bool join;
	bool notCond;
	string comp;
	int const_int;
	string const_str;
	set<string> in_set;
};

void buildTables() {
	freopen("schema.sql", "r", stdin);
	string token;
	while (cin >> token) {
		if (token == "CREATE") {
			cin >> token;
			cin >> token;
			string tableName = token;
			cin >> token;
			Table *table = new Table();
			tables[tableName] = table;
			tableNum ++;
			vector<string> colNames;
			while (1) {
				bool endline = false;
				cin >> token;
				Column *col = new Column();
				tables[tableName]->cols[token] = col;
				//cout << token << endl;
				colNames.push_back(token);
				cin >> token;
				if (token[token.length() - 1] == ',') {
					endline = true;
					token.pop_back();
				}
				if (token == "integer")
					col->type = integer;
				else
					col->type = text;
				while (!endline) {
					cin >> token;
					if (token[token.length() - 1] == ',')
						endline = true;
					if (token == ");")
						endline = true;
				}
				if (token == ");")
					break;
			}
			ifstream load("imdb/" + tableName + ".csv");
			//cout << tableName << endl;
			string data;
			int cnt = 0;
			table->sample_size = 0;
			while (getline(load, data)) {
				cnt ++;
				if (cnt > 1000 && ((rand() & 32767) > 32768 * rate))
					continue;
				int cursor = 0;
				for (int i = 0; i < colNames.size(); i ++) {
					string colName = colNames[i];
					string inputstr = "";
					int inputint = 0;
					bool type = table->cols[colName]->type;
					bool quote = false;
					if (data[cursor] == '"') {
						cursor ++;
						quote = true;
					}
					while (1) {
						if (cursor >= data.length()) break;
						if (data[cursor] == ',' && !quote) break;
						if (quote && data[cursor] == '"') {
							cursor ++;
							break;
						}
						if (type == integer)
							inputint = inputint * 10 + data[cursor] - '0';
						else
							inputstr.push_back(data[cursor]);
						cursor ++;
					}
					cursor ++;
					if (type == integer) {
						table->cols[colName]->sampling_int.push_back(inputint);
						if (table->cols[colName]->int_pos.find(inputint) == table->cols[colName]->int_pos.end())
							table->cols[colName]->int_pos[inputint] = unordered_set<int>();
						table->cols[colName]->int_pos[inputint].insert(table->sample_size);
					} else {
						table->cols[colName]->sampling_str.push_back(inputstr);
						if (table->cols[colName]->str_pos.find(inputstr) == table->cols[colName]->str_pos.end())
							table->cols[colName]->str_pos[inputstr] = unordered_set<int>();
						table->cols[colName]->str_pos[inputstr].insert(table->sample_size);
					}
				}
				table->sample_size ++;
			}
			table->size = cnt;
			//cout << table->size << " " << table->sample_size << endl;
			load.close();
		}
	}
}

char readChar(string query, int *cursor) {
	while (query[*cursor] == ' ') {
		(*cursor) ++;
		if (*cursor >= query.length()) return '\0';
	}
	return query[*cursor];
}
string readVar(string query, int *cursor) {
	if (*cursor >= query.length()) return "";
	//cout << query.length() << endl;
	while ((query[*cursor] < 'a' || query[*cursor] > 'z') && (query[*cursor] != '_')) {
		(*cursor) ++;
		//cout << *cursor << " " << (*cursor >= query.length()) << endl;
		if (*cursor >= query.length()) return "";
	}
	//cout << query[*cursor] << endl;
	string var = "";
	while ((query[*cursor] >= 'a' && query[*cursor] <= 'z') || (query[*cursor] == '_')) {
		var.push_back(query[*cursor]);
		(*cursor) ++;
		if (*cursor >= query.length()) break;
	}
	return var;
}
string readComp(string query, int *cursor) {
	if (*cursor >= query.length()) return "";
	while (query[*cursor] == ' ') {
		(*cursor) ++;
		if (*cursor >= query.length()) return "";
	}
	if (query[*cursor] == 'N') {
		(*cursor) += 3;
		return "NOT";
	}
	if (query[*cursor] == 'L') {
		(*cursor) += 4;
		return "LIKE";
	}
	if (query[*cursor] == 'B') {
		(*cursor) += 7;
		return "BETWEEN";
	}
	if (query[*cursor] == 'I') {
		(*cursor) += 2;
		return "IN";
	}
	if (query[*cursor] == '<') {
		(*cursor) ++;
		if (query[*cursor] == '=') {
			(*cursor) ++;
			return "<=";
		} else
			return "<";
	}
	if (query[*cursor] == '>') {
		(*cursor) ++;
		if (query[*cursor] == '=') {
			(*cursor) ++;
			return ">=";
		} else
			return ">";
	}
	if (query[*cursor] == '=') {
		(*cursor) ++;
		return "=";
	}
	if (query[*cursor] == '!') {
		(*cursor) += 2;
		return "!=";
	}
}
string readStr(string query, int *cursor) {
	while (query[*cursor] != '\'') (*cursor) ++;
	(*cursor) ++;
	string str = "";
	while (query[*cursor] != '\'') {
		str.push_back(query[*cursor]);
		(*cursor) ++;
	}
	(*cursor) ++;
	return str;
}
int readInt(string query, int *cursor) {
	while (query[*cursor] < '0' || query[*cursor] > '9') (*cursor) ++;
	int num = 0;
	while ((*cursor < query.length()) && (query[*cursor] >= '0' && query[*cursor] <= '9')) {
		num = num * 10 + query[*cursor] - '0';
		(*cursor) ++;
	}
	return num;
}

inline bool cmp(pair<string, string> a, pair<string, string> b) {
	return tables[a.second]->sample_size < tables[b.second]->sample_size;
}

inline bool like(string s, string pattern) {
	for (int i = 0; i < pattern.length(); i ++)
		if (pattern[i] == '%')
			pattern = pattern.replace(i, 1, ".*");
	regex r(pattern);
	return regex_match(s, r);
}

/*inline bool testEqual(Table *table1, string col1, int id1, Table *table2, string col2, int id2) {
	if (table1->cols[col1]->type == integer)
		return table1->cols[col1]->sampling_int[id1] == table2->cols[col2]->sampling_int[id2];
	else
		return table1->cols[col1]->sampling_str[id1] == table2->cols[col2]->sampling_str[id2];
}*/
inline bool testCond(Condition *cond, Table *table, int id) {
	bool type = table->cols[cond->col1]->type;
	bool answer;
	if (cond->comp == "IN")
		answer = cond->in_set.find(table->cols[cond->col1]->sampling_str[id]) != cond->in_set.end();
	else {
		int val_int;
		string val_str;
		if (type == integer)
			val_int = table->cols[cond->col1]->sampling_int[id];
		else
			val_str = table->cols[cond->col1]->sampling_str[id];
		if (cond->comp == "=") {
			if (type == integer)
				answer = val_int == cond->const_int;
			else
				answer = val_str == cond->const_str;
		} else if (cond->comp == "!=") {
			if (type == integer)
				answer = val_int != cond->const_int;
			else
				answer = val_str != cond->const_str;
		} else if (cond->comp == "<") {
			if (type == integer)
				answer = val_int < cond->const_int;
			else
				answer = val_str < cond->const_str;
		} else if (cond->comp == "<=" ) {
			if (type == integer)
				answer = val_int <= cond->const_int;
			else
				answer = val_str <= cond->const_str;
		} else if (cond->comp == ">" ) {
			if (type == integer)
				answer = val_int > cond->const_int;
			else
				answer = val_str > cond->const_str;
		} else if (cond->comp == ">=" ) {
			if (type == integer)
				answer = val_int >= cond->const_int;
			else
				answer = val_str >= cond->const_str;
		} else if (cond->comp == "LIKE")
			answer = like(val_str, cond->const_str);
	}
	if (cond->notCond) answer = !answer;
	return answer;
}

void Query(string sqlFile, string outFile) {
	freopen(sqlFile.c_str(), "r", stdin);
	ofstream output(outFile);
	while (1) {
		char c;
		string query = "";
		bool flag = false;
		while ((c = getchar()) != EOF) {
			if (c == ';') break;
			if (c > 20) {
				query.push_back(c);
				flag = true;
			} else query.push_back(' ');
		}
		if (!flag) break;
		//cout << query << endl;
		
		int tableStart = query.find("FROM") + 5;
		int condStart = query.find("WHERE") + 5;
		vector<pair<string, string> > joinName;
		string joinTables = query.substr(tableStart, condStart - tableStart - 6);
		//cout << joinTables << endl;
		string cond = query.substr(condStart);
		
		for (int i = 0; i < joinTables.length(); i ++)
			if (joinTables[i] == ',') joinTables[i] = ' ';
		stringstream parser;
		parser << joinTables;
		string token;
		double mul = 1.;
		while (parser >> token) {
			string table = token;
			parser >> token;
			if (token == "AS") parser >> token;
			joinName.push_back(make_pair(token, table));
			mul *= (double)tables[table]->size / tables[table]->sample_size;
		}
		sort(joinName.begin(), joinName.end(), cmp);
		
		int cursor = 0;
		vector<Condition*> conds;
		//cout << cond << endl;
		while (1) {
			string table1 = readVar(cond, &cursor);
			//cout << table1 << " ";
			if (table1 == "") break;
			Condition *condition = new Condition();
			conds.push_back(condition);
			string col1 = readVar(cond, &cursor);
			//cout << col1 << endl;
			condition->table1 = table1; condition->col1 = col1;
			string comp = readComp(cond, &cursor);
			condition->notCond = false;
			if (comp == "NOT") {
				condition->notCond = true;
				comp = readComp(cond, &cursor);
			}
			//cout << comp << endl;
			condition->comp = comp;
			char c = readChar(cond, &cursor);
			if (c >= 'a' && c <= 'z')
				condition->join = true;
			else
				condition->join = false;
			if (condition->join) {
				condition->table2 = readVar(cond, &cursor);
				condition->col2 = readVar(cond, &cursor);
			} else if (comp == "!=" || comp == "<" || comp == ">" || comp == "=" || comp == "<=" || comp == ">=" || comp == "LIKE") {
				if (c == '\'')
					condition->const_str = readStr(cond, &cursor);
				else
					condition->const_int = readInt(cond, &cursor);
			} else if (comp == "BETWEEN") {
				c = readChar(cond, &cursor);
				condition->comp = ">=";
				if (c == '\'')
					condition->const_str = readStr(cond, &cursor);
				else
					condition->const_int = readInt(cond, &cursor);
				readChar(cond, &cursor);
				cursor += 3;
				Condition *condition2 = new Condition();
				conds.push_back(condition2);
				condition2->table1 = condition->table1; condition2->col1 = condition->col1;
				condition2->join = false; condition2->comp = "<="; condition2->notCond = false;
				if (c == '\'')
					condition2->const_str = readStr(cond, &cursor);
				else
					condition2->const_int = readInt(cond, &cursor);
			} else if (comp == "IN") {
				while (readChar(cond, &cursor) != ')') {
					//cout << readChar(cond, &cursor) << endl;
					condition->in_set.insert(readStr(cond, &cursor));
				}
				cursor ++;
			}
			bool flag = false;
			while (1) {
				c = readChar(cond, &cursor);
				//cout << c << endl;
				if (c == 'A') {
					cursor += 3;
					break;
				}
				if (c == '\0') {
					flag = true;
					break;
				}
			}
			if (flag) break;
		}
		
		//cout << conds.size() << endl;
		vector<vector<int> > results, temp;
		for (int i = 0; i < joinName.size(); i ++) {
			vector<int> joinTables;
			vector<string> joinCol1, joinCol2;
			for (int j = 0; j < conds.size(); j ++) {
				if (conds[j]->table1 == joinName[i].first && conds[j]->join)
					for (int k = 0; k < i; k ++)
						if (joinName[k].first == conds[j]->table2) {
							joinTables.push_back(k);
							joinCol1.push_back(conds[j]->col1);
							joinCol2.push_back(conds[j]->col2);
							break;
						}
				if (conds[j]->table2 == joinName[i].first && conds[j]->join)
					for (int k = 0; k < i; k ++)
						if (joinName[k].first == conds[j]->table1) {
							joinTables.push_back(k);
							joinCol1.push_back(conds[j]->col2);
							joinCol2.push_back(conds[j]->col1);
							break;
						}
			}
			Table *table = tables[joinName[i].second];
			unordered_set<int> rows;
			int cnt = 0;
			for (int j = 0; j < table->sample_size; j ++) {
				bool flag = true;
				//cout << j << " ";
				for (int k = 0; k < conds.size(); k ++) {
					//cout << conds[k]->comp << " " << conds[k]->const_int << endl;
					if (conds[k]->table1 == joinName[i].first && conds[k]->join == false)
						if (!testCond(conds[k], table, j)) {
							flag = false;
							break;
						}
				}
				if (flag) {
					if (i == 0) {
						vector<int> v;
						v.push_back(j);
						results.push_back(v);
					} else
						rows.insert(j);
					cnt ++;
				}
			}
			//cout << "inter: " << results.size() << " " << cnt << endl;
			if (i == 0)
				continue;
			vector<vector<int> >().swap(temp);
			for (int k = 0; k < results.size(); k ++) {
				bool flag = true;
				vector<int> valid;
				for (int l = 0; l < joinTables.size(); l ++) {
					if (table->cols[joinCol1[l]]->type == integer) {
						int val = tables[joinName[joinTables[l]].second]->cols[joinCol2[l]]->sampling_int[results[k][joinTables[l]]];
						if (table->cols[joinCol1[l]]->int_pos.find(val) == table->cols[joinCol1[l]]->int_pos.end()) {
							valid.clear();
							break;
						}
						unordered_set<int> &pos = table->cols[joinCol1[l]]->int_pos[val];
						if (l == 0) {
							for (auto it = pos.begin(); it != pos.end(); it ++)
								if (rows.find(*it) != rows.end())
									valid.push_back(*it);
						} else {
							for (auto it = valid.begin(); it != valid.end(); it ++)
								if (pos.find(*it) == pos.end())
									it = valid.erase(it);
						}
					} else {
						string val = tables[joinName[joinTables[l]].second]->cols[joinCol2[l]]->sampling_str[results[k][joinTables[l]]];
						if (table->cols[joinCol1[l]]->str_pos.find(val) == table->cols[joinCol1[l]]->str_pos.end()) {
							valid.clear();
							break;
						}
						unordered_set<int> &pos = table->cols[joinCol1[l]]->str_pos[val];
						if (l == 0) {
							for (auto it = pos.begin(); it != pos.end(); it ++)
								if (rows.find(*it) != rows.end())
									valid.push_back(*it);
						} else {
							for (auto it = valid.begin(); it != valid.end(); it ++)
								if (pos.find(*it) == pos.end())
									it = valid.erase(it);
						}
					}
				}
				for (int j = 0; j < valid.size(); j ++) {
					temp.push_back(results[k]);
					temp[temp.size() - 1].push_back(valid[j]);
				}
			}
			vector<vector<int> >().swap(results);
			results = temp;
			if ((int)results.size() > threshold) {
				int orig = results.size();
				for (auto it = results.begin(); it != results.end(); it ++)
					if ((rand() & 32767) > 32768 * ((double)threshold / orig))
						it = results.erase(it);
				mul *= (double)orig / results.size();
			}
		}
		
		output << (long long)((double)results.size() * mul) << endl;
		vector<vector<int> >().swap(results);
	}
	output.close();
}

int main() {
	ios::sync_with_stdio(false);
	
	clock_t st = clock();
	buildTables();
	clock_t t1 = clock();
	cout << "loaded data: " << (double)(t1 - st) / CLOCKS_PER_SEC << "s" << endl;
	
	Query("easy.sql", "easy.out");
	clock_t t2 = clock();
	cout << "easy: " << (double)(t2 - t1) / CLOCKS_PER_SEC << "s" << endl;
	Query("middle.sql", "middle.out");
	clock_t t3 = clock();
	cout << "middle: " << (double)(t3 - t2) / CLOCKS_PER_SEC << "s" << endl;
	Query("hard.sql", "hard.out");
	clock_t t4 = clock();
	cout << "hard: " << (double)(t4 - t3) / CLOCKS_PER_SEC << "s" << endl;
	
	return 0;
}
