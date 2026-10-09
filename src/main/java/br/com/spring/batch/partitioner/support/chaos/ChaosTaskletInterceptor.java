package br.com.spring.batch.partitioner.support.chaos;

import java.util.Optional;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("chaos")
public class ChaosTaskletInterceptor implements BeanPostProcessor, MethodInterceptor {

    private static final String EXECUTE = "execute";
    private static final int NO_ATTEMPT = 0;

    private final ObjectProvider<FailureInjector> failureInjector;
    private final ObjectProvider<OriginalFileRepository> originals;

    public ChaosTaskletInterceptor(ObjectProvider<FailureInjector> failureInjector,
                                   ObjectProvider<OriginalFileRepository> originals) {
        this.failureInjector = failureInjector;
        this.originals = originals;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!(bean instanceof Tasklet)) {
            return bean;
        }
        ProxyFactory proxy = new ProxyFactory(bean);
        proxy.setProxyTargetClass(true);
        proxy.addAdvice(this);
        return proxy.getProxy();
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (!EXECUTE.equals(invocation.getMethod().getName())) {
            return invocation.proceed();
        }
        StepExecution step = ((StepContribution) invocation.getArguments()[0]).getStepExecution();
        Optional<ChaosPoint> point = ChaosPoint.ofStep(step.getStepName());
        if (point.isEmpty()) {
            return invocation.proceed();
        }
        if (!point.get().afterStep()) {
            check(point.get(), step);
            return invocation.proceed();
        }
        Object result = invocation.proceed();
        check(point.get(), step);
        return result;
    }

    private void check(ChaosPoint point, StepExecution step) {
        String fileId = FilePartitionJob.fileIdOf(step);
        int attempt = originals.getObject().findById(fileId).map(ReceivedFileDocument::attempts).orElse(NO_ATTEMPT);
        if (point == ChaosPoint.PARTITION) {
            int partitionIndex = PartitionRange.from(step.getExecutionContext()).index();
            failureInjector.getObject().checkPartition(fileId, attempt, partitionIndex);
            return;
        }
        failureInjector.getObject().check(point, fileId, attempt);
    }
}
