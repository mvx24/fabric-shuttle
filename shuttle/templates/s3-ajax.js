/* global $:false, define:false, exports:false */
(function ({% if module_name %}module{% endif %}) {
	'use strict';

	function generateRandomName() {
		return (new Date()).getTime() + '-' + Math.floor((Math.random() * 999999) + 1);
	}

	function generateGuid() {
		// Adapted from http://stackoverflow.com/questions/105034/create-guid-uuid-in-javascript
		function s4() {
			var time = (new Date()).getTime() % 100000;
			var hex = (Math.floor((1 + Math.random()) * 0x10000) ^ time).toString(16).substring(1);
			return '0000'.substr(0, 4 - hex.length) + hex;
		}
		return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
	}

	function dataURItoBlob(dataURI, contentType) {
		var binary = atob(dataURI.split(',')[1]);
		var array = [];
		contentType = contentType || 'image/jpeg';
		for(var i = 0; i < binary.length; i++) {
			array.push(binary.charCodeAt(i));
		}
		array = new Uint8Array(array);
		return new Blob([array.buffer], {type: contentType});
	}
{% for policy in policies %}
	function save{{ policy.name|capfirst }}(filename, blob, {% for name in policy.meta_data %}{{ name }}, {% endfor %}settings) {
		var dat = new FormData();
		dat.append('key', {% if policy.directory %}'{{ policy.directory }}' + {% endif %}filename);
		dat.append('AWSAccessKeyId', '{{ policy.access_key_id }}');{% if policy.acl %}
		dat.append('acl', '{{ policy.acl }}');{% endif %}{% if policy.success_action_redirect %}
		dat.append('success_action_redirect', '{{ policy.success_action_redirect }}');{% endif %}{% for header, value in policy.headers.items %}
		{% if header == 'Content-Type' %}dat.append('{{ header }}', {% if value|length == 0 or value|last == '/' %}blob.type{% else %}'{{ value }}'{% endif %});{% else %}dat.append('{{ header }}', '{{ value }}');{% endif %}{% endfor %}{% for name in policy.meta_data %}
		dat.append('x-amz-meta-{{ name }}', {{ name }});{% endfor %}
		dat.append('policy', '{{ policy.encoded }}');
		dat.append('signature', '{{ policy.signature }}');
		dat.append('file', blob);
		return $.ajax($.extend({
			type: 'POST',
			url: 'https://s3.amazonaws.com/{{ policy.bucket }}/',
			data: dat,
			cache: false,
			contentType: false,
			processData: false
		}, settings));
	}
{% endfor %}
	var methods = {
		generateRandomName: generateRandomName,
		generateGuid: generateGuid,
		dataURItoBlob: dataURItoBlob,{% for policy in policies %}
		save{{ policy.name|capfirst }}: save{{ policy.name|capfirst }}{% if not forloop.last %},{% endif %}{% endfor %}
	};
	{% if module_name %}$.extend(module, methods);
	{% else %}
	if(typeof define === 'function' && define.amd) {
		// AMD
		define(methods);
	}
	else if (typeof exports === 'object') {
		// CommonJS
		$.extend(exports, methods);
	}
	else {
		// Global
		$.extend(window, methods);
	}
	{% endif %}
})({{ module_name }});
