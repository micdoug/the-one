/*
 * Copyright (C) 2016 Michael Dougras da Silva
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package routing;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.Settings;
import util.Tuple;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * A simple implementation of Groups NET forwarding algorithm.
 * This implementation use pre calculated routes loaded from external files.
 */
public class GroupsNetRouter extends ActiveRouter {

    /**
     * The base path to load nodes route files.
     */
    private static final String ROUTES_FILE_CONFIG = "routesFile";

    private static final Map<Integer, Set<Integer>> routes = new HashMap<>();

    /**
     * Constructor.
     *
     * @param set The settings object.
     * @param namespace The base namespace for router configurations.
     */
    public GroupsNetRouter(Settings settings) {
        super(settings);

        settings = new Settings("GroupsNetRouter");
        String file_path = settings.getSetting(ROUTES_FILE_CONFIG);
        System.out.println(String.format("Trying to open the file %s", file_path));
        FileReader file_reader = null;
        BufferedReader reader = null;
        try {
            file_reader = new FileReader(file_path);
            reader = new BufferedReader(file_reader);
            String line = reader.readLine();
            while (line != null) {
                String[] values = line.split(" ");
                int source_node = Integer.parseInt(values[0]);
                HashSet<Integer> route = new HashSet<>();
                for (int i=1; i<values.length; ++i) {
                    route.add(Integer.parseInt(values[i]));
                }
                routes.put(source_node, route);
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            System.out.println(String.format("The file %s was not found", file_path));
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            System.out.println(String.format("Error while reading the file %s. Details: %s",
                    file_path, e.getMessage()));
            System.exit(1);
        }
        finally {
            try {
                assert reader != null;
                reader.close();
                file_reader.close();
            } catch (IOException e) {
                // Nothing to do here
            }
        }
    }

    /**
     * Copy constructor.
     * @param prot Prototype.
     */
    public GroupsNetRouter(GroupsNetRouter prot) {
        super(prot);

        // Nothing yet
    }

    @Override
    public void update() {
        super.update();

        if (!canStartTransfer() || isTransferring()) {
            return; // Nothing to transfer or is currently transferring
        }

        // Try messages that could be delivered to final recipient
        if (exchangeDeliverableMessages() != null) {
            return;
        }

        tryOtherMessages();
    }

    @Override
    public MessageRouter replicate() {
        return new GroupsNetRouter(this);
    }

    /**
     * Process each message using community and rank information to decide
     * if the message should be forwarded or not.
     * @return A set of messages and connections to forward messages.
     */
    protected Tuple<Message, Connection> tryOtherMessages() {
        List<Tuple<Message, Connection>> messages = new ArrayList<Tuple<Message, Connection>>();

        Collection<Message> msgCollection = getMessageCollection();

        /**
         * Process all messages
         */
        for (Connection con : getConnections()) {
            // Get the reference to the router of the other node
            DTNHost otherNode = con.getOtherNode(getHost());
            final GroupsNetRouter otherRouter = (GroupsNetRouter) otherNode.getRouter();

            if (otherRouter.isTransferring()) {
                continue; // skip hosts that are transferring
            }

            for (Message message : msgCollection) {
                if (otherRouter.hasMessage(message.getId())) {
                    continue; // skip messages that the other one has
                }
                HashSet<Integer> route = (HashSet<Integer>) message.getProperty("route");
                if (route.contains(otherNode.getAddress())) {
                    messages.add(new Tuple<Message, Connection>(message, con));
                }
            }
        }
        if (messages.isEmpty()) {
            return null;
        }

        return tryMessagesForConnected(messages);
    }

    @Override
    public boolean createNewMessage(Message m) {
        // When creating the message we set the best nodes to forward the message to.
        int id = Integer.parseInt(m.getId());
        m.addProperty("route", routes.get(id));
        return super.createNewMessage(m);
    }
}
