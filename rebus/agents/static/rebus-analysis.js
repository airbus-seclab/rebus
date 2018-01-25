
$(document).ready(function() {
    if (!window.console) window.console = {};
    if (!window.console.log) window.console.log = function() {};
    splitpath = location.pathname.split('/');

    if (splitpath.length == 4) {
        updater.domain = splitpath[2];
        updater.uuid = splitpath[3];
        updater.cursor = 'all';
        updater.poll();
    }
});

function getCookie(name) {
    var r = document.cookie.match('\\b' + name + '=([^;]*)\\b');
    return r ? r[1] : undefined;
}

jQuery.postJSON = function(url, args, callback) {
    args._xsrf = getCookie('_xsrf');
    $.ajax({url: url, data: $.param(args), dataType: 'text', type: 'POST',
        success: function(response) {
            if (callback) callback(eval('(' + response + ')'));
        }, error: function(response) {
            console.log('ERROR:', response)
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
        this.attr('disabled', 'disabled');
    } else {
        this.removeAttr('disabled');
    }
    return this;
};

var links = {}

var updater = {
    errorSleepTime: 500,
    cursor: null,
    currentAjaxQuery: null,
    domain: null,
    uuid: null,
    filenametext: null,

    reset: function() {
        updater.stopPolling();
        $('#inbox').html('');
        updater.filenametext = null;
        $('#filename-uuid').hide();
        updater.domain = 'default';
        updater.cursor = 'cached';
    },

    stopPolling: function(reset) {
        if (updater.currentAjaxQuery) {
            updater.currentAjaxQuery.abort();
        }
    },

    poll: function() {
        var args = {'_xsrf': getCookie('_xsrf'), 'page': 'analysis',
                    'domain': updater.domain, 'uuid': updater.uuid};
        if (updater.cursor) args.cursor = updater.cursor;
        updater.stopPolling();
        updater.currentAjaxQuery = $.ajax({url: '/poll_descriptors',
            type: 'POST',
            dataType: 'text',
            data: $.param(args),
            success: updater.onSuccess,
            error: updater.onError,
            complete: updater.onComplete});
    },

    onSuccess: function(response) {
        try {
            updater.newDescriptors(eval('(' + response + ')'));
        } catch (e) {
            updater.onError();
            return;
        }
        updater.errorSleepTime = 500;
        window.setTimeout(updater.poll, 0);
    },

    onError: function(response, errortype) {
        if (errortype == 'abort') {
            return;
        }
        updater.errorSleepTime *= 2;
        console.log('Poll error; sleeping for', updater.errorSleepTime, 'ms');
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
        if (!updater.filenametext) {
            updater.filenametext = descriptor.label;
            $('#display-filename').html(descriptor.label);
            $('#display-uuid').html('<a href="/analysis/default/' + updater.uuid + '">' + updater.uuid + '</a>');
            $('#display-domain').html(descriptor.domain);
            $('#filename-uuid').fadeIn();
            $('#helptext').hide();
        }
        var blockname = descriptor.agent;
        if (descriptor.selector.indexOf('/link/') == 0) {
            blockname = 'links';
            if (!(descriptor.linksrchash in links)) {
                links[descriptor.linksrchash] = {};
            }
            links[descriptor.linksrchash][descriptor.hash] = descriptor.html;
            var linkicon = $('.linkicon', '#m' + descriptor.linksrchash);
            linkicon.fadeTo(100, 0.25).fadeTo(300, 1.0);
            linkicon.popover({
                trigger: 'focus',
                content: function(t) {
                    res = '<table class="table table-striped table-condensed"><thead><th>linked to</th><th>reason</th></thead>';
                    ls = links[descriptor.linksrchash];
                    for (var link in ls) {
                        res += ls[link];
                    }
                    res += '</table>';
                    return res;
                },
                html: true}).click(function(e){ // fix for chrome
                    e.preventDefault();
                    $(this).focus();
                    });
        }
        var container = $('#container_' + blockname);
        if (container.length == 0) {
            container = $('#template_container').clone().attr('id', 'container_' + blockname);
                container.find('.panel-title').text(blockname);
                $('#inbox').append(container);
        }
        inbox = container.find('.container-inbox');

        var node = $(descriptor.html);
        var incoming_hash = node[0].id;
        var existing = inbox.find("#"+incoming_hash);
        if (existing.length > 0) {
            existing.replaceWith(node);
        } else {
            node.hide();
            inbox.append(node);
            node.fadeIn();
        }
    },
};

$(document).on('click', function(event) {
    var target = $(event.target);
    if (target.hasClass('glyphicon-cog')) {
        p = target.parent();
        pp = p.parent();
        params = {'domain': unescape(pp.attr('data-domain')), 'selector': unescape(pp.attr('data-selector'))};
        $.ajax({
            url: '/processing/list_processors',
            type: 'POST',
            dataType: 'text',
                data: params,
                success: function(response) {
                    p.popover('destroy');
                    p.popover({
                        content: response,
                        html: true,
                        trigger: 'focus'});
                    p.popover('show');
                }
        });
    }
    if (target.hasClass('request-process-link')) {
        pp = target.closest('.container-key');
        params = {'domain': unescape(pp.attr('data-domain')), 'selector': unescape(pp.attr('data-selector')), 'targets': [target.text()]};
        $.ajax({
            url: '/processing/request',
            type: 'POST',
            dataType: 'text',
            data: JSON.stringify(params)
        });
    }
});


