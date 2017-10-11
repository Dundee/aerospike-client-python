/*******************************************************************************
 * Copyright 2013-2017 Aerospike, Inc.
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

#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_info.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>
#include <aerospike/as_cluster.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

/**
 ********************************************************************************************************
 * Macros for Info API.
 ********************************************************************************************************
 */
#define MAX_HOST_COUNT 128
#define INFO_REQUEST_RESPONSE_DELIMITER "\t"
#define INFO_RESPONSE_END "\n"
#define HOST_DELIMITER ";"
#define IP_PORT_DELIMITER ":"

/**
 ******************************************************************************************************
 * Returns data for a particular request string to AerospikeClient_InfoNode
 *
 * @param self                  AerospikeClient object
 * @param request_str_p         Request string sent from the python client
 * @param py_host               Optional host sent from the python client
 * @param py_policy             The policy sent from the python client
 *
 * Returns information about a host.
 ********************************************************************************************************/
static PyObject * AerospikeClient_InfoNode_Invoke(
	as_error * err, AerospikeClient * self,
	PyObject * py_request_str, PyObject * py_host, PyObject * py_policy) {

	PyObject * py_response = NULL;
	PyObject * py_ustr = NULL;
	PyObject * py_ustr1 = NULL;
	as_policy_info info_policy;
	as_policy_info* info_policy_p = NULL;
	as_host * host = NULL;
	char* address =NULL;
	long port_no;
	char* response_p = NULL;
	as_status status = AEROSPIKE_OK;

	if (!self || !self->as) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	if (self->as->config.hosts->size == 0) {
		as_error_update(err, AEROSPIKE_ERR_CLUSTER, "No hosts in configuration");
		goto CLEANUP;
	}

	host = (as_host *)as_vector_get(self->as->config.hosts, 0);
	address = host->name;
	port_no = host->port;

	if (py_policy) {
		if (PyDict_Check(py_policy)) {
			pyobject_to_policy_info(err, py_policy, &info_policy, &info_policy_p,
					&self->as->config.policies.info);
			if (err->code != AEROSPIKE_OK) {
				goto CLEANUP;
			}
		} else {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "Policy should be a dictionary");
			goto CLEANUP;
		}
	}

	if (py_host) {
		if (PyTuple_Check(py_host) && PyTuple_Size(py_host) == 2) {
			PyObject * py_addr = PyTuple_GetItem(py_host,0);
			PyObject * py_port = PyTuple_GetItem(py_host,1);

			if (PyString_Check(py_addr)) {
				address = PyString_AsString(py_addr);
			} else if (PyUnicode_Check(py_addr)) {
				py_ustr = PyUnicode_AsUTF8String(py_addr);
				address = PyBytes_AsString(py_ustr);
			}
			if (PyInt_Check(py_port)) {
				port_no = (uint16_t) PyInt_AsLong(py_port);
			}
			else if (PyLong_Check(py_port)) {
				port_no = (uint16_t) PyLong_AsLong(py_port);
			}
		} else if (!PyTuple_Check(py_host)) {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "Host should be a specified in form of Tuple.");
			goto CLEANUP;
		}
	}

	char * request_str_p = NULL;
	if (PyUnicode_Check(py_request_str)) {
		py_ustr1 = PyUnicode_AsUTF8String(py_request_str);
		request_str_p = PyBytes_AsString(py_ustr1);
	} else if (PyString_Check(py_request_str)) {
		request_str_p = PyString_AsString(py_request_str);
	} else {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Request should be of string type");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	status = aerospike_info_host(self->as, err, info_policy_p,
		(const char *) address, (uint16_t) port_no, request_str_p,
		&response_p);
	Py_END_ALLOW_THREADS
	if (err->code == AEROSPIKE_OK) {
		if (response_p && status == AEROSPIKE_OK) {
			py_response = PyString_FromString(response_p);
			free(response_p);
		} else if (!response_p) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid info operation");
			goto CLEANUP;
		} else if (status != AEROSPIKE_OK) {
			as_error_update(err, status, "Info operation failed");
			goto CLEANUP;
		}
	} else {
		as_error_update(err, err->code, err->message);
		goto CLEANUP;
	}

CLEANUP:

	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (py_ustr1) {
		Py_DECREF(py_ustr1);
	}

	if (err->code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return py_response;
}

/**
 ******************************************************************************************************
 * Returns data about a particular node in the database depending upon the request string.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns information about a host.
 ********************************************************************************************************/
PyObject * AerospikeClient_InfoNode(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_host = NULL;
	PyObject * py_policy = NULL;

	PyObject * py_request = NULL;

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"command", "host", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:info_node", kwlist,
				&py_request, &py_host, &py_policy) == false) {
		return NULL;
	}

	return AerospikeClient_InfoNode_Invoke(&err, self, py_request, py_host, py_policy);

}

	/**
	 ******************************************************************************************************
	 * Returns data about the nodes to AerospikeClient_GetNodes.
	 *
	 * @param self                  AerospikeClient object
	 *
	 * Returns a list containing the details of the nodes.
	 ********************************************************************************************************/
	static PyObject * AerospikeClient_GetNodes_Invoke(
		AerospikeClient * self) {

		PyObject* py_hostname = NULL;
		PyObject* py_port = NULL;
		PyObject* return_value = PyList_New(0);

		as_nodes* nodes = NULL;
		char* hostname = NULL;
		char* split_point = NULL;

		as_error err;
		as_error_init(&err);

		if (!self || !self->as) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
			goto CLEANUP;
		}

		if (!self->is_conn_16) {
			as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
			goto CLEANUP;
		}

		aerospike* as = self->as;
		as_cluster* cluster = as->cluster;
		// If the cluster goes down between the last call and this call, this could theoretically occur.
		if (!cluster) {
			as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "invalid aerospike cluster");
			goto CLEANUP;
		}

		nodes = as_nodes_reserve(cluster);

		if (!nodes) {
			as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "Cluster is empty");
			goto CLEANUP;
		}

		for (uint32_t i = 0; i < nodes->size; i++) {
			as_node* node = nodes->array[i];
			hostname = (char*)as_node_get_address_string(node);

			split_point = strrchr(hostname, ':');
			if (!split_point) {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Malformed host name string");
				goto CLEANUP;
			}
			Py_ssize_t host_length = split_point - hostname;
			py_hostname = PyString_FromStringAndSize(hostname, host_length);

			if (!py_hostname) {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to create python hostname");
				goto CLEANUP;
			}

			// convert "3000" -> 3000, using base 10 | use long since it works in 2 & 3
			py_port = PyLong_FromString(split_point + 1, NULL, 10);
			if (!py_port || PyErr_Occurred()) {
				// py_port exists
				Py_XDECREF(py_hostname);
				as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Non numeric port found");
				goto CLEANUP;
			}
			PyObject* py_host_pair = Py_BuildValue("OO", py_hostname, py_port);

			Py_XDECREF(py_port);
			Py_XDECREF(py_hostname);

			if(!py_host_pair) {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to build node info tuple");
				goto CLEANUP;
			}

			PyList_Append(return_value, py_host_pair);
			Py_DecRef(py_host_pair);
		}
	CLEANUP:
		if(nodes) {
			as_nodes_release(nodes);
		}
		if (err.code != AEROSPIKE_OK) {
			// Clear the return value if it exists
			Py_XDECREF(return_value);
			PyObject * py_err = NULL;
			error_to_pyobject(&err, &py_err);
			PyObject *exception_type = raise_exception(&err);
			PyErr_SetObject(exception_type, py_err);
			Py_DECREF(py_err);

			return NULL;
		}

		return return_value;
	}

/******************************************************************************************************
 * Returns data about the nodes in a cluster of the database.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list containing the details of the nodes.
 ********************************************************************************************************/
PyObject * AerospikeClient_GetNodes(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	return AerospikeClient_GetNodes_Invoke(self);
}
