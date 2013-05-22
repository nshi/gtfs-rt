/*
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.vertx.mods

import org.codehaus.jackson.map.ObjectMapper

def mapper = new ObjectMapper()
def pcr = mapper.reader(VoltInvocation.class)

def eb = vertx.eventBus
def log = container.logger
def hosts = container.config.get('hosts',['localhost'])

def volt = new VoltClient(hosts,log)

eb.registerHandler("volt") { message ->
    def invocation = pcr.readValue(message.body)
    volt.callProcedure( invocation) { response ->
        message.reply response.toJSONString()
    }
}

def vertxStop() {
    log.info "stopping volt bus module"
    volt.shutdown()
}



