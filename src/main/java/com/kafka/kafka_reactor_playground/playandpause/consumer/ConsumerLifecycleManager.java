package com.kafka.kafka_reactor_playground.playandpause.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ConsumerLifecycleManager {
    Logger logger = LoggerFactory.getLogger(ConsumerLifecycleManager.class);
    private static final String CHANNEL_NAME = "consumer-in-0";
    private final BindingsLifecycleController bindingsLifecycleController;

    public ConsumerLifecycleManager(BindingsLifecycleController bindingsLifecycleController) {
        this.bindingsLifecycleController = bindingsLifecycleController;
    }

    public void pause() {
        this.bindingsLifecycleController.changeState(CHANNEL_NAME, State.PAUSED);
    }

    public void resume() {
        this.bindingsLifecycleController.changeState(CHANNEL_NAME, State.RESUMED);
    }

    @Scheduled(fixedDelay = 10000, initialDelay = 5000)
    public void checkDependency() {
        var isPaused = this.bindingsLifecycleController.queryState(CHANNEL_NAME)
                .stream().allMatch(Binding::isPaused);

        if (isPaused) {
            logger.info("resuming channel " + CHANNEL_NAME);
            this.resume();
        } else {
            logger.info("pausing channel " + CHANNEL_NAME);
            this.pause();
        }

    }

}
