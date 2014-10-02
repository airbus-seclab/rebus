
$(document).ready(function() {
    if (!window.console) window.console = {};
    if (!window.console.log) window.console.log = function() {};

});

function getCookie(name) {
    var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
    return r ? r[1] : undefined;
}

jQuery.postJSON = function(url, args, callback) {
    args._xsrf = getCookie("_xsrf");
    $.ajax({url: url, data: $.param(args), dataType: "text", type: "POST",
        success: function(response) {
            if (callback) callback(eval("(" + response + ")"));
        }, error: function(response) {
            console.log("ERROR:", response)
        }});
};

jQuery.fn.formToDict = function() {
    var fields = this.serializeArray();
    var json = {}
    for (var i = 0; i < fields.length; i++) {
        json[fields[i].name] = fields[i].value;
    }
    if (json.next) delete json.next;
    return json;
};

jQuery.fn.disable = function() {
    this.enable(false);
    return this;
};

jQuery.fn.enable = function(opt_enable) {
    if (arguments.length && !opt_enable) {
        this.attr("disabled", "disabled");
    } else {
        this.removeAttr("disabled");
    }
    return this;
};

var updater = {
    errorSleepTime: 500,
    cursor: null,
    currentAjaxQuery: null,
    domain: null,
    uuid: null,

    stopPolling: function() {
        if (updater.currentAjaxQuery) {
            updater.currentAjaxQuery.abort();
        }
    },

    poll: function() {
        var args = {"_xsrf": getCookie("_xsrf"), "page": "analysis",
                    "domain": updater.domain, "uuid": updater.uuid};
        if (updater.cursor) args.cursor = updater.cursor;
        updater.stopPolling();
        updater.currentAjaxQuery = $.ajax({url: "/poll_descriptors",
            type: "POST",
            dataType: "text",
            data: $.param(args),
            success: updater.onSuccess,
            error: updater.onError,
            complete: updater.onComplete});
    },

    onSuccess: function(response) {
        try {
            updater.newDescriptors(eval("(" + response + ")"));
        } catch (e) {
            updater.onError();
            return;
        }
        updater.errorSleepTime = 500;
        window.setTimeout(updater.poll, 0);
    },

    onError: function(response, errortype) {
        if (errortype == "abort") {
            return;
        }
        updater.errorSleepTime *= 2;
        console.log("Poll error; sleeping for", updater.errorSleepTime, "ms");
        window.setTimeout(updater.poll, updater.errorSleepTime);
    },

    onComplete: function() {
        updater.currentAjaxQuery = null;
    },

    newDescriptors: function(response) {
        if (!response.descrinfos) return;
        var descrinfos = response.descrinfos;
        updater.cursor = descrinfos[descrinfos.length - 1].hash;
        for (var i = 0; i < descrinfos.length; i++) {
            updater.showDescriptor(descrinfos[i]);
        }
    },

    showDescriptor: function(descriptor) {
        var container = $("#container_" + descriptor.agent);
        if (container.length == 0) {
            container = $('#template_container').clone().attr('id', 'container_' + descriptor.agent)
                container.find('.panel-title').text(descriptor.agent)
                $("#inbox").append(container)
        }
        var node = $(descriptor.html);
        inbox = container.find('.container-inbox');
        var existing = inbox.find("#m" + descriptor.hash);
        if (existing.length > 0) {
            existing.replaceWith(node);
        } else {
            node.hide();
            inbox.append(node);
            node.fadeIn();
        }
    }
};

$(function () {
    $('#fileupload').fileupload({
        dataType: 'json',
        add: function(e, data) {
            updater.stopPolling();
            $('#progress .progress-bar').css('width', '0%');
            $('.upload-status').text('Uploading file ' + data.files[0].name + '...');
            $('.upload-status-panel').show();
            $('#inbox').html('');
            updater.domain = 'default';
            updater.cursor = 'any';
            data.submit();
        },
        dataType: 'json',
        done: function(e, data) {
            $('.upload-status').text('File ' + data.files[0].name + ' has successfully been uploaded.');
            $('#progress .progress-bar').css('width', '100%');
            $('.upload-status-panel').delay(2000).hide(200);
            updater.uuid = data.result.uuid;
            updater.poll();
        },
        progressall: function (e, data) {
            var progress = parseInt(data.loaded / data.total * 100, 10);
            $('#progress .progress-bar').css(
                'width',
                progress + '%'
                );
        }
    });
});

