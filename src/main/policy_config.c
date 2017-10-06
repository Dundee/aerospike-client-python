/*******************************************************************************
 * Copyright 2017 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
#include "policy_config.h"

as_status set_read_policy(as_policy_read* read_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;
	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_base_policy(&read_policy->base, py_policy);
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&read_policy->key, py_policy, "key");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&read_policy->replica, py_policy, "replica");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&read_policy->consistency_level, py_policy, "consistency_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&read_policy->deserialize, py_policy, "deserialize");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_write_policy(as_policy_write* write_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;

	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_base_policy(&write_policy->base, py_policy);
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&write_policy->key, py_policy, "key");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&write_policy->replica, py_policy, "replica");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&write_policy->commit_level, py_policy, "commit_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&write_policy->gen, py_policy, "gen");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&write_policy->exists, py_policy, "exists");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&write_policy->compression_threshold, py_policy, "compression_threshold");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&write_policy->durable_delete, py_policy, "durable_delete");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_apply_policy(as_policy_apply* apply_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;

	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_base_policy(&apply_policy->base, py_policy);
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&apply_policy->key, py_policy, "key");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&apply_policy->replica, py_policy, "replica");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&apply_policy->gen, py_policy, "gen");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&apply_policy->commit_level, py_policy, "commit_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_remove_policy(as_policy_remove* remove_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;

	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_base_policy(&remove_policy->base, py_policy);
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&remove_policy->key, py_policy, "key");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&remove_policy->replica, py_policy, "replica");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&remove_policy->commit_level, py_policy, "commit_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&remove_policy->gen, py_policy, "gen");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&remove_policy->durable_delete, py_policy, "durable_delete");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_query_policy(as_policy_query* query_policy, PyObject* py_policy) {

	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	return set_base_policy(&query_policy->base, py_policy);
}

as_status set_scan_policy(as_policy_scan* scan_policy, PyObject* py_policy) {

	if (!py_policy) {
		return AEROSPIKE_OK;
	}
	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	return set_base_policy(&scan_policy->base, py_policy);
}

as_status set_operate_policy(as_policy_operate* operate_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;

	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_base_policy(&operate_policy->base, py_policy);
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&operate_policy->key, py_policy, "key");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&operate_policy->replica, py_policy, "replica");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&operate_policy->commit_level, py_policy, "commit_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&operate_policy->gen, py_policy, "gen");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&operate_policy->consistency_level, py_policy, "consistency_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_batch_policy(as_policy_batch* batch_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;
	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_base_policy(&batch_policy->base, py_policy);
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&batch_policy->consistency_level, py_policy, "consistency_level");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&batch_policy->concurrent, py_policy, "concurrent");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&batch_policy->use_batch_direct, py_policy, "use_batch_direct");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&batch_policy->allow_inline, py_policy, "allow_inline");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&batch_policy->send_set_name, py_policy, "send_set_name");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_bool_property(&batch_policy->deserialize, py_policy, "deserialize");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_base_policy(as_policy_base* base_policy, PyObject* py_policy) {

	as_status status = AEROSPIKE_OK;

	if (!py_policy) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return AEROSPIKE_ERR_PARAM;
	}

	status = set_optional_uint32_property((uint32_t*)&base_policy->total_timeout, py_policy, "total_timeout");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&base_policy->socket_timeout, py_policy, "socket_timeout");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&base_policy->max_retries, py_policy, "max_retries");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = set_optional_uint32_property((uint32_t*)&base_policy->sleep_between_retries, py_policy, "sleep_between_retries");
	if (status != AEROSPIKE_OK) {
		return status;
	}

	return AEROSPIKE_OK;
}

as_status set_optional_uint32_property(uint32_t* target_ptr, PyObject* py_policy, const char* name) {
	PyObject* py_policy_val = NULL;
	long long int uint32_max = 0xFFFFFFFF;
	if (!py_policy || !PyDict_Check(py_policy)) {
		return AEROSPIKE_OK;
	}

	py_policy_val = PyDict_GetItemString(py_policy, name);
	if (!py_policy_val) {
		return AEROSPIKE_OK;
	}
	if (PyInt_Check(py_policy_val)) {
		long int_value = PyInt_AsLong(py_policy_val);

		if (int_value == -1 && PyErr_Occurred()) {
			// This wasn't a valid int, or was too large
			// We are handling the error ourselves, so clear the overflow error
			PyErr_Clear();
			return AEROSPIKE_ERR_PARAM;

		/* If the number was less than zero, or would not fit in a uint32, error */
		}
		if (int_value < 0 || int_value > uint32_max) {
			return AEROSPIKE_ERR_PARAM;
		}

		*target_ptr = (uint32_t)int_value;
		return AEROSPIKE_OK;
	}
	return AEROSPIKE_ERR_PARAM;
}

as_status set_optional_bool_property(bool* target_ptr, PyObject* py_policy, const char* name) {
	PyObject* py_policy_val = NULL;
	if (!py_policy || !PyDict_Check(py_policy)) {
		return AEROSPIKE_OK;
	}

	py_policy_val = PyDict_GetItemString(py_policy, name);
	if (!py_policy_val) {
		return AEROSPIKE_OK;
	}
	if (PyBool_Check(py_policy_val)) {
		*target_ptr = PyObject_IsTrue(py_policy_val);
		return AEROSPIKE_OK;
	}
	return AEROSPIKE_ERR_PARAM;
}
