
$(document).ready(function() {
    if (!window.console) window.console = {};
    if (!window.console.log) window.console.log = function() {};
    updater.poll();
});

function getCookie(name) {
    var r = document.cookie.match('\\b' + name + '=([^;]*)\\b');
    return r ? r[1] : undefined;
}

var known_agents = {}

var updater = {
    errorSleepTime: 2000,
    cursor: null,
    currentAjaxQuery: null,
    domain: 'default',
    filenametext: null,

    stopPolling: function() {
        if (updater.currentAjaxQuery) {
            updater.currentAjaxQuery.abort();
        }
    },

    poll: function() {
        var args = {'_xsrf': getCookie('_xsrf'), 'domain': updater.domain};
        updater.stopPolling();
        updater.currentAjaxQuery = $.ajax({url: '/agents',
            type: 'POST',
            dataType: 'text',
            data: $.param(args),
            success: updater.onSuccess,
            error: updater.onError,
            complete: updater.onComplete});
    },

    onSuccess: function(response) {
        try {
            updater.updateAgents(eval('(' + response + ')'));
        } catch (e) {
            updater.onError();
            return;
        }
        updater.errorSleepTime = 2000;
        window.setTimeout(updater.poll, 1000);
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

    updateAgents: function(response) {
        if (!response.agents_stats) return;
        var stats = response.agents_stats;
        var total = response.total
        for (var i = 0; i < stats.length; i++) {
            updater.updateAgent(stats[i][0], stats[i][1], total);
        }
    },

    updateAgent: function(name, count, total) {
        if (name in known_agents) {
            html = '<div class="progress"><div class="progress-bar" role="progressbar" aria-valuenow="' + count + '" aria-valuemin="0" aria-valuemax="' + total + '" style="width: ' + count*100/total + '%; min-width: 4em;">' + count + ' / ' + total + '</div></div>';
            known_agents[name].html(html);
        } else {
                html = '<tr id="agent-' + name + '"><td>' + name + '</td><td><div class="progress"><div class="progress-bar" role="progressbar" aria-valuenow="' + count + '" aria-valuemin="0" aria-valuemax="' + total + '" style="width: ' + count*100/total + '%; min-width: 4em;">' + count + ' / ' + total + '</div></div></td></tr>';
            $('#inbox').append(html);
            known_agents[name] = $('#inbox #agent-' + name + ' td div.progress');
        }
    },
};

