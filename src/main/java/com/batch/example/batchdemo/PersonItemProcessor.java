package com.batch.example.batchdemo;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;



public class PersonItemProcessor implements ItemProcessor<Person, Person> {
    private static Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    @Override
    public Person process(Person person) throws Exception {
        final String firstName = person.getFirstName().toUpperCase();
        final String lastName = person.getLastName().toUpperCase();
        log.info("Processing data: "+firstName);
        return new Person(firstName, lastName);
    }



}
