
$(document).ready(function() {
    if (!window.console) window.console = {};
    if (!window.console.log) window.console.log = function() {};

    updater.poll();
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
    errorSleepTime: 1000,
    cursor: null,

    poll: function() {
        var args = {"_xsrf": getCookie("_xsrf"), "page": "monitor", "domain":
                    "", "uuid": "", cursor: "cached"};
        if (updater.cursor) args.cursor = updater.cursor;
        $.ajax({url: "/poll_descriptors", type: "POST", dataType: "text",
            data: $.param(args), success: updater.onSuccess,
            error: updater.onError});
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

    onError: function(response, errorString, e) {
        updater.errorSleepTime *= 2;
        console.log("Poll error; sleeping for", updater.errorSleepTime, "ms");
        window.setTimeout(updater.poll, updater.errorSleepTime);
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
        var node = $(descriptor.html);
        node.hide();
        $("#inbox").append(node);
        node.fadeIn();
    }
};
