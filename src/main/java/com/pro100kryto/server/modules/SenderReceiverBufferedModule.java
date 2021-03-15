package com.pro100kryto.server.modules;

import com.pro100kryto.server.StartStopStatus;
import com.pro100kryto.server.logger.ILogger;
import com.pro100kryto.server.module.AModuleConnection;
import com.pro100kryto.server.module.IModuleConnectionSafe;
import com.pro100kryto.server.module.Module;
import com.pro100kryto.server.module.ModuleConnectionSafe;
import com.pro100kryto.server.modules.packetpool.connection.IPacketPoolModuleConnection;
import com.pro100kryto.server.modules.protocollitenetlib.connection.IProtocolModuleConnection;
import com.pro100kryto.server.modules.receiverbuffered.connection.IReceiverBufferedModuleConnection;
import com.pro100kryto.server.modules.sender.connection.ISenderModuleConnection;
import com.pro100kryto.server.service.IServiceControl;
import com.pro100kryto.server.utils.datagram.packets.IPacket;
import com.pro100kryto.server.utils.datagram.packets.IPacketInProcess;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SenderReceiverBufferedModule extends Module {
    private IModuleConnectionSafe<IProtocolModuleConnection> protocolModuleConnection;
    private IModuleConnectionSafe<IPacketPoolModuleConnection> packetPoolModuleConnection;

    private BlockingQueue<IPacketInProcess> packetBuffer; // thread-safe ok
    private int socketTimeout;
    private DatagramSocket datagramSocket;
    private int port;

    public SenderReceiverBufferedModule(IServiceControl service, String name) {
        super(service, name);
    }

    @Override
    protected void startAction() throws Throwable {
        if (moduleConnection == null){
            moduleConnection = new SRModuleConnection(logger, name, type);
        }

        String protocolModuleName = settings.getOrDefault("protocol-module-name", "Protocol");
        protocolModuleConnection = new ModuleConnectionSafe<>(service, protocolModuleName);
        protocolModuleConnection.refreshConnection();

        String packetPoolModuleName = settings.getOrDefault("packetpool-module-name", "PacketPool");
        packetPoolModuleConnection = new ModuleConnectionSafe<>(service, packetPoolModuleName);
        packetPoolModuleConnection.refreshConnection();

        int packetBufferSize = Integer.parseInt(
                settings.getOrDefault("packetbuffer-size", "128"));
        packetBuffer = new ArrayBlockingQueue<>(packetBufferSize);

        port = Integer.parseInt(settings.getOrDefault("socket-port","49300"));
        socketTimeout = Integer.parseInt(settings.getOrDefault("socket-timeout", "1500"));

        datagramSocket = new DatagramSocket(port);
        datagramSocket.setSoTimeout(socketTimeout);
    }

    @Override
    protected void stopAction(boolean force) throws Throwable {
        datagramSocket.close();
        packetBuffer.clear();

        protocolModuleConnection = null;
        packetPoolModuleConnection = null;

        packetBuffer = null;
    }

    @Override
    public void tick() throws Throwable {
        IPacketInProcess packetInProcess = packetPoolModuleConnection.getModuleConnection().getNextPacket();
        try {
            try {
                packetInProcess.receive(datagramSocket);
                if (!packetBuffer.offer(packetInProcess, socketTimeout, TimeUnit.MILLISECONDS)) {
                    logger.writeWarn("packetBuffer is full");
                }

                return;
            } catch (NullPointerException nullPointerException){
                logger.writeWarn("PacketPool is empty");

            } catch (SocketTimeoutException ignored){
            }

            packetInProcess.recycle();

        } catch (Throwable throwable){
            if (packetInProcess!=null) packetInProcess.recycle();
            throw throwable;
        }
    }

    private final class SRModuleConnection extends AModuleConnection
            implements IReceiverBufferedModuleConnection, ISenderModuleConnection {

        public SRModuleConnection(ILogger logger, String moduleName, String moduleType) {
            super(logger, moduleName, moduleType);
        }

        @Override
        public boolean isAliveModule() {
            return getStatus() == StartStopStatus.STARTED;
        }

        @Override
        public boolean hasNextPacket() {
            try {
                return !packetBuffer.isEmpty();
            } catch (NullPointerException ignored){
            }
            return false;
        }

        @Override
        public IPacket getNextPacket() {
            try{
                IPacketInProcess packetInProcess = packetBuffer.poll();
                try {
                    Objects.requireNonNull(packetInProcess);
                    protocolModuleConnection.getModuleConnection().processPacketOnReceive(packetInProcess);
                    return packetInProcess.convertToFinalPacket();

                } catch (NullPointerException ignored){

                } catch (Throwable throwable){
                    logger.writeException(throwable);
                    packetInProcess.recycle();
                }

            } catch (Throwable ignored){
            }

            return null;
        }

        @Override
        public void sendPacketAndRecycle(IPacketInProcess packetInProcess) {
            sendPacket(packetInProcess);
            packetInProcess.recycle();
        }

        @Override
        public void sendPacket(IPacketInProcess packetInProcess) {
            try {
                IPacket packet = protocolModuleConnection.getModuleConnection().processPacketOnSend(packetInProcess)
                        .convertToFinalPacket();
                DatagramPacket datagramPacket = packet.createDatagramPacket();
                datagramSocket.send(datagramPacket);

            } catch (Throwable throwable) {
                logger.writeException(throwable, "failed send packet");
            }
        }
    }
}
