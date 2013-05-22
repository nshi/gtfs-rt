import org.vertx.groovy.core.http.RouteMatcher
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

def logger = container.logger
def eb = vertx.eventBus

def rm = new RouteMatcher()

def formUrlEncodedToMap(buff) {
    buff.toString().split('&').collect {
        it.split('=').collect { URLDecoder.decode(it) }
    }.findAll { it.size == 2 }.collectEntries()
}

String invokeAs( String pname, String...pargs) {
    JsonBuilder invocationBuilder = new JsonBuilder()
    invocationBuilder {
        name pname
        params pargs
    }
    invocationBuilder.toString()
}

def parseResponse(jsonResp) {
    new JsonSlurper().parseText(jsonResp)
}

Boolean checkError(req, resp) {
    if (resp.status != 1) {
        req.response.with {
            statusCode = 400
            statusMessage = resp.statusstring
            end()
        }
        return true
    } else {
        return false
    }
}

def getResults(resp) {
    resp.results.data[0]
}

String positionsTableToJson(table) {
    JsonBuilder builder = new JsonBuilder()
    if (table) {
        builder {
            tripId table[0]
            lat    table[1]
            lng    table[2]
        }
    }
    builder.toString()
}

// Find the next trip of the route that's gonna arrive at the stop
rm.get('/api/route/') { req ->
    logger.info "Get next trip for route " + req.params
    eb.send("volt", invokeAs("FindNextTrip",
                             req.params["routeId"],
                             req.params["stopName"],
                             Long.toString(System.currentTimeMillis()))) { reply ->
        resp = parseResponse(reply.body)
        if ( ! checkError(req, resp)) {
            logger.info "results " + getResults(resp)
            req.response.headers["Content-Type"] = 'application/json; charset=UTF-8'
            req.response.end getResults(resp)
        }
    }
}

// Subscribe to notifications of the given trip at the given stop
rm.get('/api/trip/subscribe/') { req ->
    logger.info "Subscribing to " + req.params
    eb.send("volt", invokeAs("SubscribeToTrip",
                             req.params["tripId"],
                             req.params["startDate"],
                             req.params["stopoId"],
                             req.params["delay"])) { reply ->
        resp = parseResponse(reply.body)
        if ( ! checkError(req, resp)) {
            logger.info "results " + getResults(resp)
            req.response.headers["Content-Type"] = 'application/json; charset=UTF-8'
            req.response.end getResults(resp)
        }
    }
}

// Find the position of the given trip
rm.get('/api/trip/position/') { req ->
    logger.info req.params
    logger.info "getting lastest schedule " + req.params
//    eb.send("volt", invokeAs("GetLatestSchedule", params.tripId)) { reply ->
    eb.send("volt", invokeAs("@AdHoc", "select trip_id, latitude, longitude from vehicle_positions where trip_id = '19957644';")) { reply ->
        resp = parseResponse(reply.body)
        if ( ! checkError(req, resp)) {
            logger.info "results " + getResults(resp)
            req.response.headers["Content-Type"] = 'application/json; charset=UTF-8'
            req.response.end positionsTableToJson(getResults(resp))
        }
    }
}

rm.post('/') { req ->
    req.bodyHandler { body ->
        def params = formUrlEncodedToMap(body).subMap(['key','value'])
        eb.send("volt", invokeAs("Put", params.key, params.value)) { reply ->
            req.response.headers["Content-Type"] = 'application/json; charset=UTF-8'
            req.response.end reply.body
        }
    }
}

// Serve static files
rm.getWithRegEx('^\\/.*') { req ->
    def file = req.path == "/" ? "index.html" : req.path.substring(1);
    req.response.sendFile('../ui/' + file)
}

container.deployModule('com.voltdb.persistor-v1.0',[ hosts: ['localhost'] ], 1) { deplID ->
    logger.info "VoltDB module deployment id: ${deplID}"
}

vertx.createHttpServer().requestHandler(rm.asClosure()).listen(9000)
